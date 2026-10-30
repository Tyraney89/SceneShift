//! Terminal UI for connecting to OBS and switching the program scene.

use std::time::Duration;

use futures_util::StreamExt;
use obws::Client;
use obws::client::{ConnectConfig, DEFAULT_BROADCAST_CAPACITY};
use obws::requests::EventSubscription;
use obws::requests::inputs::{InputId, Volume};
use obws::requests::scenes::SceneId;
use ratatui::crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, List, ListItem, ListState, Padding, Paragraph};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const VOLUME_STEP: f32 = 0.05;
const NEON_ORANGE: Color = Color::Rgb(255, 95, 0);
const NEON_PINK: Color = Color::Rgb(255, 45, 166);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    tokio::spawn(obs_worker(commands_rx, events_tx));

    let mut app = App::from_env();
    if std::env::var("SERVERIP").is_ok() {
        app.connect(&commands_tx);
    }

    let mut terminal = match ratatui::try_init() {
        Ok(terminal) => terminal,
        Err(err) => {
            ratatui::restore();
            return Err(err.into());
        }
    };
    let result = run(&mut terminal, &mut app, &commands_tx, &mut events_rx).await;
    ratatui::restore();
    result.map_err(Into::into)
}

async fn run(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    commands: &mpsc::UnboundedSender<Command>,
    obs_events: &mut mpsc::UnboundedReceiver<ObsEvent>,
) -> std::io::Result<()> {
    let mut term_events = EventStream::new();

    loop {
        terminal.draw(|frame| draw(frame, app))?;

        let connecting = matches!(app.conn, ConnState::Connecting);
        tokio::select! {
            biased;
            obs = obs_events.recv() => {
                let Some(event) = obs else { break };
                app.apply(event);
            }
            term = term_events.next() => {
                match term {
                    Some(Ok(event)) => {
                        if handle_input(app, commands, event) {
                            break;
                        }
                    }
                    Some(Err(_)) => {}
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)), if connecting => {
                app.spinner = app.spinner.wrapping_add(1);
            }
        }
    }

    Ok(())
}

enum Command {
    Connect { epoch: u64, creds: Credentials },
    Disconnect { epoch: u64 },
    Refresh { epoch: u64 },
    SetScene { epoch: u64, name: String },
    SetVolume { epoch: u64, name: String, mul: f32 },
    ToggleMute { epoch: u64, name: String },
}

enum ObsEvent {
    Connected {
        epoch: u64,
        creds: Credentials,
        snapshot: SceneSnapshot,
    },
    Snapshot {
        epoch: u64,
        snapshot: SceneSnapshot,
    },
    Switched {
        epoch: u64,
        name: String,
    },
    AudioLevel {
        epoch: u64,
        source: AudioSource,
    },
    ConnectFailed {
        epoch: u64,
        message: String,
    },
    RequestFailed {
        epoch: u64,
        message: String,
    },
    Lost {
        epoch: u64,
        message: String,
    },
}

#[derive(Clone, PartialEq, Eq)]
struct Credentials {
    host: String,
    port: u16,
    password: String,
}

#[derive(Clone, PartialEq, Eq)]
struct SceneEntry {
    name: String,
    index: usize,
}

#[derive(Clone, Debug)]
struct AudioSource {
    name: String,
    mul: f32,
    muted: bool,
}

struct SceneSnapshot {
    scenes: Vec<SceneEntry>,
    current: Option<String>,
    audio: Vec<AudioSource>,
}

struct ConnectedSession {
    client: Client,
    snapshot: SceneSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Focus {
    Host,
    Port,
    Password,
    Action,
    Scenes,
    Audio,
}

impl Focus {
    fn is_text(self) -> bool {
        matches!(self, Self::Host | Self::Port | Self::Password)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConnState {
    Disconnected,
    Connecting,
    Connected,
    Failed(String),
}

enum Notice {
    Empty,
    Info(String),
    Ok(String),
    Err(String),
}

impl Notice {
    fn text(&self) -> &str {
        match self {
            Self::Empty => "",
            Self::Info(text) | Self::Ok(text) | Self::Err(text) => text,
        }
    }

    fn style(&self) -> Style {
        match self {
            Self::Empty => Style::new(),
            Self::Info(_) => Style::new().fg(Color::Cyan),
            Self::Ok(_) => Style::new().fg(Color::Green),
            Self::Err(_) => Style::new().fg(Color::Red),
        }
    }
}

struct Field {
    value: String,
    cursor: usize,
}

impl Field {
    fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        let cursor = value.chars().count();
        Self { value, cursor }
    }

    fn insert(&mut self, ch: char, max_chars: usize) {
        if self.value.chars().count() >= max_chars {
            return;
        }
        let byte = byte_index(&self.value, self.cursor);
        self.value.insert(byte, ch);
        self.cursor += 1;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor -= 1;
        let start = byte_index(&self.value, self.cursor);
        let end = byte_index(&self.value, self.cursor + 1);
        self.value.replace_range(start..end, "");
    }

    fn delete_char(&mut self) {
        let start = byte_index(&self.value, self.cursor);
        if start >= self.value.len() {
            return;
        }
        let end = byte_index(&self.value, self.cursor + 1);
        self.value.replace_range(start..end, "");
    }

    fn move_cursor(&mut self, delta: isize) {
        let len = self.value.chars().count() as isize;
        self.cursor = (self.cursor as isize + delta).clamp(0, len) as usize;
    }

    fn home(&mut self) {
        self.cursor = 0;
    }

    fn end(&mut self) {
        self.cursor = self.value.chars().count();
    }

    fn clear(&mut self) {
        self.value.clear();
        self.cursor = 0;
    }
}

struct App {
    host: Field,
    port: Field,
    password: Field,
    show_password: bool,
    focus: Focus,
    editing: bool,
    pending_g: bool,
    epoch: u64,
    conn: ConnState,
    active: Option<Credentials>,
    attempt: Option<Credentials>,
    scenes: Vec<SceneEntry>,
    current: Option<String>,
    audio: Vec<AudioSource>,
    notice: Notice,
    spinner: usize,
    list_state: ListState,
    audio_state: ListState,
}

impl App {
    fn new(host: String, port: String, password: String, focus: Focus) -> Self {
        Self {
            host: Field::new(host),
            port: Field::new(port),
            password: Field::new(password),
            show_password: false,
            focus,
            editing: false,
            pending_g: false,
            epoch: 0,
            conn: ConnState::Disconnected,
            active: None,
            attempt: None,
            scenes: Vec::new(),
            current: None,
            audio: Vec::new(),
            notice: Notice::Empty,
            spinner: 0,
            list_state: ListState::default(),
            audio_state: ListState::default(),
        }
    }

    fn from_env() -> Self {
        load_env();
        let host_var = std::env::var("SERVERIP");
        let host = host_var.clone().unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("PORT").unwrap_or_else(|_| "4455".to_string());
        let password = std::env::var("PASSWORD").unwrap_or_default();
        let focus = if host_var.is_ok() {
            Focus::Action
        } else {
            Focus::Host
        };
        Self::new(host, port, password, focus)
    }

    fn connect(&mut self, commands: &mpsc::UnboundedSender<Command>) {
        let host = self.host.value.trim().to_string();
        if host.is_empty() {
            self.reject(Focus::Host, "Host is required");
            return;
        }
        let port = match parse_port(&self.port.value) {
            Ok(port) => port,
            Err(message) => {
                self.reject(Focus::Port, message);
                return;
            }
        };

        let creds = Credentials {
            host,
            port,
            password: self.password.value.clone(),
        };
        self.epoch = self.epoch.wrapping_add(1);
        self.conn = ConnState::Connecting;
        self.spinner = 0;
        self.active = None;
        self.attempt = Some(creds.clone());
        self.scenes.clear();
        self.current = None;
        self.audio.clear();
        self.notice = Notice::Empty;
        self.select(0);
        self.select_audio(0);
        let _ = commands.send(Command::Connect {
            epoch: self.epoch,
            creds,
        });
    }

    fn disconnect(&mut self, commands: &mpsc::UnboundedSender<Command>) {
        self.epoch = self.epoch.wrapping_add(1);
        self.conn = ConnState::Disconnected;
        self.active = None;
        self.attempt = None;
        self.scenes.clear();
        self.current = None;
        self.audio.clear();
        self.notice = Notice::Empty;
        self.select(0);
        self.select_audio(0);
        let _ = commands.send(Command::Disconnect { epoch: self.epoch });
    }

    fn activate(&mut self, commands: &mpsc::UnboundedSender<Command>) {
        if matches!(self.conn, ConnState::Connecting)
            || (matches!(self.conn, ConnState::Connected) && self.credentials_match())
        {
            self.disconnect(commands);
        } else {
            self.connect(commands);
        }
    }

    fn refresh_or_connect(&mut self, commands: &mpsc::UnboundedSender<Command>) {
        match self.conn {
            ConnState::Connecting => {}
            ConnState::Connected => {
                let _ = commands.send(Command::Refresh { epoch: self.epoch });
            }
            ConnState::Disconnected | ConnState::Failed(_) => self.connect(commands),
        }
    }

    fn reject(&mut self, focus: Focus, message: &str) {
        self.focus = focus;
        if matches!(self.conn, ConnState::Connected) {
            self.notice = Notice::Err(message.to_string());
        } else {
            self.notice = Notice::Empty;
            self.conn = ConnState::Failed(message.to_string());
        }
    }

    fn credentials_match(&self) -> bool {
        let Some(active) = &self.active else {
            return false;
        };
        let Ok(port) = parse_port(&self.port.value) else {
            return false;
        };
        active.host == self.host.value.trim()
            && active.port == port
            && active.password == self.password.value
    }

    fn action_label(&self) -> &'static str {
        if matches!(self.conn, ConnState::Connecting) {
            "Cancel"
        } else if matches!(self.conn, ConnState::Connected) && self.credentials_match() {
            "Disconnect"
        } else if matches!(self.conn, ConnState::Connected) {
            "Reconnect"
        } else {
            "Connect"
        }
    }

    fn apply(&mut self, event: ObsEvent) {
        match event {
            ObsEvent::Connected {
                epoch,
                creds,
                snapshot,
            } => {
                if epoch != self.epoch {
                    return;
                }
                let live = snapshot.current.clone();
                self.conn = ConnState::Connected;
                self.active = Some(creds);
                self.attempt = None;
                self.notice = Notice::Empty;
                self.replace_scenes(snapshot);
                self.select_name(live.as_deref());
                if self.focus == Focus::Action {
                    self.focus = Focus::Scenes;
                }
            }
            ObsEvent::Snapshot { epoch, snapshot } => {
                if epoch != self.epoch {
                    return;
                }
                let keep = self.selected_name();
                let live = snapshot.current.clone();
                self.replace_scenes(snapshot);
                self.select_name(keep.as_deref().or(live.as_deref()));
            }
            ObsEvent::AudioLevel { epoch, source } => {
                if epoch != self.epoch {
                    return;
                }
                if let Some(existing) = self.audio.iter_mut().find(|item| item.name == source.name)
                {
                    *existing = source;
                }
            }
            ObsEvent::Switched { epoch, name } => {
                if epoch != self.epoch {
                    return;
                }
                self.current = Some(name.clone());
                self.notice = Notice::Ok(format!("Switched to {name}"));
            }
            ObsEvent::RequestFailed { epoch, message } => {
                if epoch != self.epoch {
                    return;
                }
                self.notice = Notice::Err(message);
            }
            ObsEvent::ConnectFailed { epoch, message } | ObsEvent::Lost { epoch, message } => {
                if epoch != self.epoch {
                    return;
                }
                self.fail_connection(message);
            }
        }
    }

    fn fail_connection(&mut self, message: String) {
        self.conn = ConnState::Failed(message);
        self.active = None;
        self.attempt = None;
        self.scenes.clear();
        self.current = None;
        self.audio.clear();
        self.notice = Notice::Empty;
        self.select(0);
        self.select_audio(0);
        if matches!(self.focus, Focus::Scenes | Focus::Audio) {
            self.focus = Focus::Action;
        }
    }

    fn replace_scenes(&mut self, snapshot: SceneSnapshot) {
        self.current = snapshot.current;
        self.scenes = snapshot.scenes;
        self.set_audio(snapshot.audio);
    }

    fn set_audio(&mut self, audio: Vec<AudioSource>) {
        let keep = self.selected_audio_name();
        self.audio = audio;
        let index = keep
            .and_then(|name| self.audio.iter().position(|source| source.name == name))
            .unwrap_or(0);
        self.select_audio(index);
    }

    fn selected_audio_name(&self) -> Option<String> {
        self.audio_state
            .selected()
            .and_then(|index| self.audio.get(index))
            .map(|source| source.name.clone())
    }

    fn select_audio(&mut self, index: usize) {
        if self.audio.is_empty() {
            self.audio_state.select(None);
            return;
        }
        self.audio_state
            .select(Some(index.min(self.audio.len() - 1)));
    }

    fn move_audio(&mut self, delta: isize) {
        if self.audio.is_empty() {
            self.audio_state.select(None);
            return;
        }
        let current = self.audio_state.selected().unwrap_or(0) as isize;
        let last = self.audio.len() as isize - 1;
        let next = (current + delta).clamp(0, last) as usize;
        self.audio_state.select(Some(next));
    }

    fn nudge_volume(&mut self, direction: i32, commands: &mpsc::UnboundedSender<Command>) {
        if !matches!(self.conn, ConnState::Connected) {
            self.notice = Notice::Err("Connect to OBS before changing audio".to_string());
            return;
        }
        let Some(index) = self.audio_state.selected() else {
            return;
        };
        let Some(source) = self.audio.get_mut(index) else {
            return;
        };
        let next = step_volume(source.mul, direction);
        if (next - source.mul).abs() < f32::EPSILON {
            return;
        }
        source.mul = next;
        let name = source.name.clone();
        let percent = volume_percent(next);
        self.notice = Notice::Info(format!("{name} {percent}%"));
        let _ = commands.send(Command::SetVolume {
            epoch: self.epoch,
            name,
            mul: next,
        });
    }

    fn toggle_selected_mute(&mut self, commands: &mpsc::UnboundedSender<Command>) {
        if !matches!(self.conn, ConnState::Connected) {
            self.notice = Notice::Err("Connect to OBS before changing audio".to_string());
            return;
        }
        let Some(index) = self.audio_state.selected() else {
            return;
        };
        let Some(source) = self.audio.get_mut(index) else {
            return;
        };
        source.muted = !source.muted;
        let name = source.name.clone();
        self.notice = if source.muted {
            Notice::Info(format!("{name} muted"))
        } else {
            Notice::Ok(format!("{name} unmuted"))
        };
        let _ = commands.send(Command::ToggleMute {
            epoch: self.epoch,
            name,
        });
    }

    fn selected_name(&self) -> Option<String> {
        self.list_state
            .selected()
            .and_then(|index| self.scenes.get(index))
            .map(|scene| scene.name.clone())
    }

    fn select_name(&mut self, name: Option<&str>) {
        let index = name
            .and_then(|name| self.scenes.iter().position(|scene| scene.name == name))
            .unwrap_or(0);
        self.select(index);
    }

    fn select(&mut self, index: usize) {
        if self.scenes.is_empty() {
            self.list_state.select(None);
            return;
        }
        self.list_state
            .select(Some(index.min(self.scenes.len() - 1)));
    }

    fn move_selection(&mut self, delta: isize) {
        if self.scenes.is_empty() {
            self.list_state.select(None);
            return;
        }
        let current = self.list_state.selected().unwrap_or(0) as isize;
        let last = self.scenes.len() as isize - 1;
        let next = (current + delta).clamp(0, last) as usize;
        self.list_state.select(Some(next));
    }

    fn focus_window(&mut self, number: u8) {
        self.editing = false;
        self.pending_g = false;
        match number {
            1 => self.focus = Focus::Host,
            2 => {
                self.focus = Focus::Scenes;
                if self.list_state.selected().is_none() {
                    self.select(0);
                }
            }
            3 => {
                self.focus = Focus::Audio;
                if self.audio_state.selected().is_none() {
                    self.select_audio(0);
                }
            }
            _ => {}
        }
    }

    fn goto_edge(&mut self, bottom: bool) {
        match self.focus {
            Focus::Scenes => {
                if bottom {
                    self.select(self.scenes.len().saturating_sub(1));
                } else {
                    self.select(0);
                }
            }
            Focus::Audio => {
                if bottom {
                    self.select_audio(self.audio.len().saturating_sub(1));
                } else {
                    self.select_audio(0);
                }
            }
            Focus::Host | Focus::Port | Focus::Password | Focus::Action => {
                self.focus = if bottom { Focus::Action } else { Focus::Host };
            }
        }
    }

    fn begin_insert(&mut self, commands: &mpsc::UnboundedSender<Command>) {
        if self.focus.is_text() {
            self.editing = true;
            return;
        }
        match self.focus {
            Focus::Action => self.activate(commands),
            Focus::Scenes => self.switch_selected(commands),
            Focus::Audio => self.toggle_selected_mute(commands),
            Focus::Host | Focus::Port | Focus::Password => {}
        }
    }

    fn advance_field(&mut self) {
        self.focus = match self.focus {
            Focus::Host => Focus::Port,
            Focus::Port => Focus::Password,
            Focus::Password => Focus::Action,
            other => other,
        };
        if !self.focus.is_text() {
            self.editing = false;
        }
    }

    fn cycle(&mut self, backward: bool) {
        let order = [
            Focus::Host,
            Focus::Port,
            Focus::Password,
            Focus::Action,
            Focus::Scenes,
            Focus::Audio,
        ];
        let index = order
            .iter()
            .position(|item| *item == self.focus)
            .unwrap_or(0);
        let len = order.len();
        let next = if backward {
            (index + len - 1) % len
        } else {
            (index + 1) % len
        };
        self.focus = order[next];
    }

    fn vertical(&mut self, down: bool) {
        match self.focus {
            Focus::Scenes => self.move_selection(if down { 1 } else { -1 }),
            Focus::Audio => self.move_audio(if down { 1 } else { -1 }),
            Focus::Host if down => self.focus = Focus::Port,
            Focus::Port => self.focus = if down { Focus::Password } else { Focus::Host },
            Focus::Password => self.focus = if down { Focus::Action } else { Focus::Port },
            Focus::Action if !down => self.focus = Focus::Password,
            Focus::Action => {}
            Focus::Host => {}
        }
    }

    fn switch_selected(&mut self, commands: &mpsc::UnboundedSender<Command>) {
        if !matches!(self.conn, ConnState::Connected) {
            self.notice = Notice::Err("Connect to OBS before switching scenes".to_string());
            return;
        }
        let Some(name) = self.selected_name() else {
            return;
        };
        if self.current.as_deref() == Some(name.as_str()) {
            self.notice = Notice::Info(format!("{name} is already live"));
            return;
        }
        self.notice = Notice::Info(format!("Switching to {name}…"));
        let _ = commands.send(Command::SetScene {
            epoch: self.epoch,
            name,
        });
    }

    fn edit(&mut self, code: KeyCode, ctrl: bool) {
        let max = match self.focus {
            Focus::Host => 253,
            Focus::Port => 5,
            Focus::Password => 256,
            Focus::Action | Focus::Scenes | Focus::Audio => return,
        };
        let digits_only = self.focus == Focus::Port;
        let field = match self.focus {
            Focus::Host => &mut self.host,
            Focus::Port => &mut self.port,
            Focus::Password => &mut self.password,
            Focus::Action | Focus::Scenes | Focus::Audio => return,
        };

        match code {
            KeyCode::Left => field.move_cursor(-1),
            KeyCode::Right => field.move_cursor(1),
            KeyCode::Home => field.home(),
            KeyCode::End => field.end(),
            KeyCode::Backspace => field.backspace(),
            KeyCode::Delete => field.delete_char(),
            KeyCode::Char('a') if ctrl => field.home(),
            KeyCode::Char('e') if ctrl => field.end(),
            KeyCode::Char('u') if ctrl => field.clear(),
            KeyCode::Char(ch) if !ctrl && (!digits_only || ch.is_ascii_digit()) => {
                field.insert(ch, max);
            }
            _ => {}
        }
    }
}

fn handle_input(app: &mut App, commands: &mpsc::UnboundedSender<Command>, event: Event) -> bool {
    let Event::Key(key) = event else {
        return false;
    };
    if key.kind == KeyEventKind::Release {
        return false;
    }

    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    let alt = key.modifiers.contains(KeyModifiers::ALT);

    if ctrl && matches!(key.code, KeyCode::Char('c')) {
        return true;
    }
    if key.code == KeyCode::Esc {
        app.pending_g = false;
        app.editing = false;
        return false;
    }
    if ctrl && matches!(key.code, KeyCode::Char('t')) {
        app.show_password = !app.show_password;
        return false;
    }

    if app.editing {
        match key.code {
            KeyCode::Enter => app.advance_field(),
            KeyCode::Tab => {
                app.cycle(false);
                if !app.focus.is_text() {
                    app.editing = false;
                }
            }
            KeyCode::BackTab => {
                app.cycle(true);
                if !app.focus.is_text() {
                    app.editing = false;
                }
            }
            code => app.edit(code, ctrl),
        }
        return false;
    }

    let pending_g = app.pending_g;
    app.pending_g = false;
    if pending_g && matches!(key.code, KeyCode::Char('g')) && !ctrl && !alt {
        app.goto_edge(false);
        return false;
    }

    if key.code == KeyCode::F(5) || (matches!(key.code, KeyCode::Char('r' | 'R')) && !ctrl && !alt)
    {
        app.refresh_or_connect(commands);
        return false;
    }
    if matches!(key.code, KeyCode::Char('q' | 'Q')) && !ctrl && !alt {
        return true;
    }

    match key.code {
        KeyCode::Char(ch @ '1'..='3') if !ctrl && !alt => {
            app.focus_window(ch as u8 - b'1' + 1);
        }
        KeyCode::Char('g') if !ctrl && !alt => app.pending_g = true,
        KeyCode::Char('G') if !ctrl && !alt => app.goto_edge(true),
        KeyCode::Char('h') if !ctrl && !alt => move_horizontal(app, commands, false),
        KeyCode::Char('l') if !ctrl && !alt => move_horizontal(app, commands, true),
        KeyCode::Left => move_horizontal(app, commands, false),
        KeyCode::Right => move_horizontal(app, commands, true),
        KeyCode::Char('j' | 'J') if !ctrl && !alt => app.vertical(true),
        KeyCode::Char('k' | 'K') if !ctrl && !alt => app.vertical(false),
        KeyCode::Down => app.vertical(true),
        KeyCode::Up => app.vertical(false),
        KeyCode::Char('i') if !ctrl && !alt => app.begin_insert(commands),
        KeyCode::Enter => app.begin_insert(commands),
        KeyCode::Char('-' | '_') if app.focus == Focus::Audio && !ctrl && !alt => {
            app.nudge_volume(-1, commands);
        }
        KeyCode::Char('=' | '+') if app.focus == Focus::Audio && !ctrl && !alt => {
            app.nudge_volume(1, commands);
        }
        KeyCode::Char('m' | 'M') if app.focus == Focus::Audio && !ctrl && !alt => {
            app.toggle_selected_mute(commands);
        }
        KeyCode::PageUp if app.focus == Focus::Scenes => app.move_selection(-10),
        KeyCode::PageDown if app.focus == Focus::Scenes => app.move_selection(10),
        KeyCode::PageUp if app.focus == Focus::Audio => app.move_audio(-10),
        KeyCode::PageDown if app.focus == Focus::Audio => app.move_audio(10),
        KeyCode::Home => app.goto_edge(false),
        KeyCode::End => app.goto_edge(true),
        KeyCode::Tab => app.cycle(false),
        KeyCode::BackTab => app.cycle(true),
        _ => {}
    }
    false
}

fn move_horizontal(app: &mut App, commands: &mpsc::UnboundedSender<Command>, right: bool) {
    if app.focus == Focus::Audio {
        app.nudge_volume(if right { 1 } else { -1 }, commands);
    }
}

fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    let show_logo = area.height >= 22 && area.width >= 64;
    let (logo_area, form_area, body, help_area) = if show_logo {
        let [logo_area, form_area, body, help_area] = Layout::vertical([
            Constraint::Length(5),
            Constraint::Length(8),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .areas(area);
        (Some(logo_area), form_area, body, help_area)
    } else {
        let [form_area, body, help_area] = Layout::vertical([
            Constraint::Length(8),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .areas(area);
        (None, form_area, body, help_area)
    };
    let (scene_area, audio_area) = split_panels(body, area.width);

    if let Some(logo_area) = logo_area {
        frame.render_widget(logo(), logo_area);
    }

    let inner_width = form_area.width.saturating_sub(4) as usize;
    let form = Paragraph::new(vec![
        input_line(
            "Host",
            &app.host,
            app.focus == Focus::Host,
            app.editing,
            false,
        ),
        input_line(
            "Port",
            &app.port,
            app.focus == Focus::Port,
            app.editing,
            false,
        ),
        input_line(
            "Password",
            &app.password,
            app.focus == Focus::Password,
            app.editing,
            !app.show_password,
        ),
        button_line(app),
        detail_line(app, inner_width),
        notice_line(app, inner_width),
    ])
    .block(form_block(
        app,
        if show_logo {
            " 1 Connection "
        } else {
            " 1 SceneShift "
        },
    ));
    frame.render_widget(form, form_area);

    let block = scene_block(app);
    if app.scenes.is_empty() {
        app.list_state.select(None);
        let message = empty_scenes_message(app);
        let inner = block.inner(scene_area);
        frame.render_widget(block, scene_area);
        let [_, message_area, _] = Layout::vertical([
            Constraint::Min(0),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .areas(inner);
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                message,
                Style::new().fg(Color::DarkGray),
            )))
            .centered(),
            message_area,
        );
    } else {
        if app.list_state.selected().is_none() {
            app.list_state.select(Some(0));
        }
        let items: Vec<ListItem> = app
            .scenes
            .iter()
            .map(|scene| {
                let live = app.current.as_deref() == Some(scene.name.as_str());
                let marker = if live { "●" } else { " " };
                let style = if live {
                    Style::new().fg(Color::Green).add_modifier(Modifier::BOLD)
                } else {
                    Style::new().fg(Color::White)
                };
                ListItem::new(Line::from(Span::styled(
                    format!("{marker} {}", scene.name),
                    style,
                )))
            })
            .collect();
        let highlight = if app.focus == Focus::Scenes {
            Style::new().fg(Color::Black).bg(Color::White)
        } else {
            Style::new().fg(Color::White).bg(Color::DarkGray)
        };
        let symbol = if app.focus == Focus::Scenes {
            "▸ "
        } else {
            "  "
        };
        let list = List::new(items)
            .block(block)
            .highlight_style(highlight)
            .highlight_symbol(symbol);
        frame.render_stateful_widget(list, scene_area, &mut app.list_state);
    }

    draw_audio(frame, app, audio_area);
    frame.render_widget(help_line(app), help_area);
}

fn split_panels(
    body: ratatui::layout::Rect,
    width: u16,
) -> (ratatui::layout::Rect, ratatui::layout::Rect) {
    if width >= 88 {
        let [scenes, audio] =
            Layout::horizontal([Constraint::Percentage(58), Constraint::Min(32)]).areas(body);
        (scenes, audio)
    } else {
        let audio_height = 8.min(body.height.saturating_sub(4));
        let [scenes, audio] =
            Layout::vertical([Constraint::Min(4), Constraint::Length(audio_height)]).areas(body);
        (scenes, audio)
    }
}

fn draw_audio(frame: &mut Frame, app: &mut App, area: ratatui::layout::Rect) {
    let block = audio_block(app);
    if app.audio.is_empty() {
        app.audio_state.select(None);
        let message = match &app.conn {
            ConnState::Connecting => "Loading audio…",
            ConnState::Connected => "No audio sources",
            ConnState::Failed(_) | ConnState::Disconnected => "Connect to adjust audio",
        };
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let [_, message_area, _] = Layout::vertical([
            Constraint::Min(0),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .areas(inner);
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                message,
                Style::new().fg(Color::DarkGray),
            )))
            .centered(),
            message_area,
        );
        return;
    }

    if app.audio_state.selected().is_none() {
        app.audio_state.select(Some(0));
    }
    let items: Vec<ListItem> = app.audio.iter().map(audio_item).collect();
    let highlight = if app.focus == Focus::Audio {
        Style::new().fg(Color::Black).bg(Color::White)
    } else {
        Style::new().fg(Color::White).bg(Color::DarkGray)
    };
    let symbol = if app.focus == Focus::Audio {
        "▸ "
    } else {
        "  "
    };
    let list = List::new(items)
        .block(block)
        .highlight_style(highlight)
        .highlight_symbol(symbol);
    frame.render_stateful_widget(list, area, &mut app.audio_state);
}

fn audio_block(app: &App) -> Block<'static> {
    let title = if matches!(app.conn, ConnState::Connected) {
        format!(" 3 Audio ({}) ", app.audio.len())
    } else {
        " 3 Audio ".to_string()
    };
    let style = chrome_style(app.focus == Focus::Audio);
    Block::bordered()
        .border_style(style)
        .padding(Padding::horizontal(1))
        .title(Line::from(Span::styled(title, style)))
}

fn audio_item(source: &AudioSource) -> ListItem<'static> {
    let percent = volume_percent(source.mul);
    let level = if source.muted {
        Style::new().fg(Color::Red)
    } else if percent >= 90 {
        Style::new().fg(Color::Yellow)
    } else {
        Style::new().fg(Color::Green)
    };
    let name = if source.muted {
        Style::new().fg(Color::DarkGray)
    } else {
        Style::new().fg(Color::White)
    };
    let mut spans = vec![
        Span::styled(pad_end(&fit(&source.name, 16), 16), name),
        Span::raw(" "),
        Span::styled(volume_bar(source.mul, 12), level),
        Span::styled(format!(" {percent:>3}%"), level),
    ];
    if source.muted {
        spans.push(Span::styled(" muted", Style::new().fg(Color::Red)));
    }
    ListItem::new(Line::from(spans))
}

fn form_block(app: &App, title: &str) -> Block<'static> {
    let style = chrome_style(matches!(
        app.focus,
        Focus::Host | Focus::Port | Focus::Password | Focus::Action
    ));
    Block::bordered()
        .border_style(style)
        .padding(Padding::horizontal(1))
        .title(Line::from(Span::styled(title.to_string(), style)))
}

fn logo() -> Paragraph<'static> {
    let lines = (0..5)
        .map(|row| {
            Line::from(vec![
                Span::styled(word_row("SCENE", row), Style::new().fg(NEON_ORANGE)),
                Span::raw("  "),
                Span::styled(word_row("SHIFT", row), Style::new().fg(NEON_PINK)),
            ])
        })
        .collect::<Vec<_>>();
    Paragraph::new(lines).centered()
}

fn word_row(word: &str, row: usize) -> String {
    word.chars()
        .map(|letter| glyph(letter)[row])
        .collect::<Vec<_>>()
        .join(" ")
}

fn glyph(letter: char) -> [&'static str; 5] {
    match letter {
        'S' => [" ### ", "#    ", " ### ", "    #", " ### "],
        'C' => [" ### ", "#    ", "#    ", "#    ", " ### "],
        'E' => ["#### ", "#    ", "###  ", "#    ", "#### "],
        'N' => ["#   #", "##  #", "# # #", "#  ##", "#   #"],
        'H' => ["#   #", "#   #", "#####", "#   #", "#   #"],
        'I' => [" ### ", "  #  ", "  #  ", "  #  ", " ### "],
        'F' => ["#### ", "#    ", "###  ", "#    ", "#    "],
        'T' => ["#####", "  #  ", "  #  ", "  #  ", "  #  "],
        _ => ["     ", "     ", "     ", "     ", "     "],
    }
}

fn scene_block(app: &App) -> Block<'static> {
    let title = if matches!(app.conn, ConnState::Connected) {
        format!(" 2 Scenes ({}) ", app.scenes.len())
    } else {
        " 2 Scenes ".to_string()
    };
    let style = chrome_style(app.focus == Focus::Scenes);
    let mut block = Block::bordered()
        .border_style(style)
        .padding(Padding::horizontal(1))
        .title(Line::from(Span::styled(title, style)));
    if let Some(name) = &app.current {
        block = block.title_bottom(
            Line::from(Span::styled(
                format!(" live: {name} "),
                Style::new().fg(Color::Green),
            ))
            .right_aligned(),
        );
    }
    block
}

fn chrome_style(focused: bool) -> Style {
    if focused {
        Style::new().fg(NEON_PINK)
    } else {
        Style::new().fg(NEON_ORANGE)
    }
}

fn input_line(
    label: &str,
    field: &Field,
    focused: bool,
    editing: bool,
    mask: bool,
) -> Line<'static> {
    let label_style = if focused {
        Style::new().fg(Color::White).add_modifier(Modifier::BOLD)
    } else {
        Style::new().fg(Color::DarkGray)
    };
    let mut spans = vec![Span::styled(format!("{label:<10}"), label_style)];
    spans.extend(value_spans(field, focused && editing, mask));
    Line::from(spans)
}

fn value_spans(field: &Field, focused: bool, mask: bool) -> Vec<Span<'static>> {
    let shown: String = if mask {
        "•".repeat(field.value.chars().count())
    } else {
        field.value.clone()
    };
    let plain = Style::new().fg(Color::White);
    if !focused {
        return vec![Span::styled(shown, plain)];
    }

    let cursor = field.cursor.min(shown.chars().count());
    let (before, rest) = split_chars(&shown, cursor);
    let mut rest_chars = rest.chars();
    let cursor_text = rest_chars
        .next()
        .map(|ch| ch.to_string())
        .unwrap_or_else(|| " ".to_string());
    let after: String = rest_chars.collect();
    vec![
        Span::styled(before.to_string(), plain),
        Span::styled(cursor_text, Style::new().fg(Color::Black).bg(Color::White)),
        Span::styled(after, plain),
    ]
}

fn button_line(app: &App) -> Line<'static> {
    let button_style = if app.focus == Focus::Action {
        Style::new()
            .fg(Color::Black)
            .bg(Color::White)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::new().fg(Color::White).bg(Color::DarkGray)
    };
    Line::from(vec![
        Span::styled(format!(" {} ", app.action_label()), button_style),
        Span::raw("  "),
        status_span(app),
    ])
}

fn status_span(app: &App) -> Span<'static> {
    match &app.conn {
        ConnState::Disconnected => Span::styled("○ Disconnected", Style::new().fg(Color::DarkGray)),
        ConnState::Connecting => {
            let frame = SPINNER[app.spinner % SPINNER.len()];
            Span::styled(
                format!("{frame} Connecting"),
                Style::new().fg(Color::Yellow),
            )
        }
        ConnState::Connected => Span::styled(
            "● Connected",
            Style::new().fg(Color::Green).add_modifier(Modifier::BOLD),
        ),
        ConnState::Failed(_) => Span::styled(
            "● Connection failed",
            Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
    }
}

fn detail_line(app: &App, width: usize) -> Line<'static> {
    let (text, style) = match &app.conn {
        ConnState::Failed(message) => (message.clone(), Style::new().fg(Color::Red)),
        ConnState::Connecting => (
            format!("Trying {}", endpoint(app)),
            Style::new().fg(Color::Yellow),
        ),
        ConnState::Connected => (endpoint(app), Style::new().fg(Color::White)),
        ConnState::Disconnected => (
            "Enter the OBS WebSocket host, port, and password".to_string(),
            Style::new().fg(Color::DarkGray),
        ),
    };
    Line::from(Span::styled(fit(&text, width), style))
}

fn notice_line(app: &App, width: usize) -> Line<'static> {
    Line::from(Span::styled(
        fit(app.notice.text(), width),
        app.notice.style(),
    ))
}

fn endpoint(app: &App) -> String {
    if let Some(creds) = app.active.as_ref().or(app.attempt.as_ref()) {
        format!("{}:{}", creds.host, creds.port)
    } else {
        format!("{}:{}", app.host.value.trim(), app.port.value.trim())
    }
}

fn empty_scenes_message(app: &App) -> &'static str {
    match &app.conn {
        ConnState::Connecting => "Loading scenes…",
        ConnState::Connected => "No scenes",
        ConnState::Failed(_) => "Scenes show up after OBS connects",
        ConnState::Disconnected => "Connect to load scenes",
    }
}

fn help_line(app: &App) -> Line<'static> {
    let key = Style::new().fg(Color::White);
    let text = Style::new().fg(Color::DarkGray);
    let mode = if app.editing {
        Span::styled("INSERT  ", Style::new().fg(Color::Green))
    } else {
        Span::styled("NORMAL  ", Style::new().fg(Color::Cyan))
    };
    let hints = if app.editing {
        vec![
            Span::styled("type", text),
            Span::styled("  Esc", key),
            Span::styled(" normal", text),
            Span::styled("  Enter", key),
            Span::styled(" next", text),
        ]
    } else {
        vec![
            Span::styled("hjkl", key),
            Span::styled(" move  ", text),
            Span::styled("i", key),
            Span::styled(" edit/select  ", text),
            Span::styled("1-3", key),
            Span::styled(" window  ", text),
            Span::styled("q", key),
            Span::styled(" quit", text),
        ]
    };
    Line::from(std::iter::once(mode).chain(hints).collect::<Vec<_>>())
}

fn load_env() {
    let _ = dotenvy::dotenv();
    if std::env::var("SERVERIP").is_err() {
        let _ = dotenvy::from_filename(concat!(env!("CARGO_MANIFEST_DIR"), "/.env"));
    }
}

fn parse_port(value: &str) -> Result<u16, &'static str> {
    match value.trim().parse::<u16>() {
        Ok(port) if port != 0 => Ok(port),
        _ => Err("Port must be between 1 and 65535"),
    }
}

fn byte_index(value: &str, char_index: usize) -> usize {
    value
        .char_indices()
        .nth(char_index)
        .map(|(index, _)| index)
        .unwrap_or(value.len())
}

fn split_chars(value: &str, char_index: usize) -> (&str, &str) {
    value.split_at(byte_index(value, char_index))
}

fn fit(text: &str, width: usize) -> String {
    let width = width.max(1);
    if text.chars().count() <= width {
        return text.to_string();
    }
    let mut truncated: String = text.chars().take(width.saturating_sub(1)).collect();
    truncated.push('…');
    truncated
}

/// OBS reports `sceneIndex` 0 at the bottom of the scenes dock.
fn step_volume(mul: f32, direction: i32) -> f32 {
    (mul + direction as f32 * VOLUME_STEP).clamp(0.0, 1.0)
}

fn volume_percent(mul: f32) -> i32 {
    (mul * 100.0).round() as i32
}

fn volume_bar(mul: f32, width: usize) -> String {
    let width = width.max(1);
    let filled = ((mul.clamp(0.0, 1.0) * width as f32).round() as usize).min(width);
    format!("[{}{}]", "=".repeat(filled), "-".repeat(width - filled))
}

fn pad_end(text: &str, width: usize) -> String {
    let extra = width.saturating_sub(text.chars().count());
    let mut out = text.to_string();
    out.extend(std::iter::repeat_n(' ', extra));
    out
}

fn order_scenes(mut scenes: Vec<SceneEntry>) -> Vec<SceneEntry> {
    scenes.sort_by(|left, right| {
        right
            .index
            .cmp(&left.index)
            .then_with(|| left.name.cmp(&right.name))
    });
    scenes
}

async fn obs_worker(
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<ObsEvent>,
) {
    let mut client: Option<Client> = None;
    let mut epoch = 0_u64;
    let mut audio_names: Vec<String> = Vec::new();
    let mut connecting: Option<JoinHandle<()>> = None;
    let (done_tx, mut done_rx) =
        mpsc::unbounded_channel::<(u64, Credentials, Result<ConnectedSession, String>)>();
    let mut poll = interval(Duration::from_secs(2));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    poll.tick().await;

    loop {
        tokio::select! {
            biased;
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    Command::Connect { epoch: next, creds } => {
                        epoch = next;
                        client = None;
                        audio_names.clear();
                        if let Some(task) = connecting.take() {
                            task.abort();
                        }
                        let tx = done_tx.clone();
                        connecting = Some(tokio::spawn(async move {
                            let result = open_session(&creds).await;
                            let _ = tx.send((next, creds, result));
                        }));
                    }
                    Command::Disconnect { epoch: next } => {
                        epoch = next;
                        client = None;
                        audio_names.clear();
                        if let Some(task) = connecting.take() {
                            task.abort();
                        }
                    }
                    Command::Refresh { epoch: requested } if requested == epoch => {
                        let outcome = match client.as_ref() {
                            Some(current) => {
                                Some(snapshot_with_audio(current, &mut audio_names, true).await)
                            }
                            None => None,
                        };
                        if let Some(result) = outcome
                            && !send_snapshot(&events, epoch, result)
                        {
                            client = None;
                            audio_names.clear();
                        }
                    }
                    Command::SetScene { epoch: requested, name } if requested == epoch => {
                        let outcome = match client.as_ref() {
                            Some(current) => Some(set_scene(current, &name).await),
                            None => None,
                        };
                        match outcome {
                            Some(Ok(())) => {
                                let _ = events.send(ObsEvent::Switched { epoch, name });
                            }
                            Some(Err(message)) if connection_died(&message) => {
                                client = None;
                                let _ = events.send(ObsEvent::Lost { epoch, message });
                            }
                            Some(Err(message)) => {
                                let _ = events.send(ObsEvent::RequestFailed { epoch, message });
                            }
                            None => {
                                let _ = events.send(ObsEvent::RequestFailed {
                                    epoch,
                                    message: "Not connected".to_string(),
                                });
                            }
                        }
                    }
                    Command::SetVolume {
                        epoch: requested,
                        name,
                        mul,
                    } if requested == epoch => {
                        let outcome = match client.as_ref() {
                            Some(current) => Some(change_volume(current, &name, mul).await),
                            None => None,
                        };
                        if !send_audio_result(&events, epoch, outcome, &mut client) {
                            audio_names.clear();
                        }
                    }
                    Command::ToggleMute {
                        epoch: requested,
                        name,
                    } if requested == epoch => {
                        let outcome = match client.as_ref() {
                            Some(current) => Some(change_mute(current, &name).await),
                            None => None,
                        };
                        if !send_audio_result(&events, epoch, outcome, &mut client) {
                            audio_names.clear();
                        }
                    }
                    Command::Refresh { .. }
                    | Command::SetScene { .. }
                    | Command::SetVolume { .. }
                    | Command::ToggleMute { .. } => {}
                }
            }
            done = done_rx.recv() => {
                let Some((done_epoch, creds, result)) = done else { break };
                if done_epoch != epoch {
                    continue;
                }
                connecting = None;
                match result {
                    Ok(session) => {
                        audio_names = session
                            .snapshot
                            .audio
                            .iter()
                            .map(|source| source.name.clone())
                            .collect();
                        client = Some(session.client);
                        let _ = events.send(ObsEvent::Connected {
                            epoch,
                            creds,
                            snapshot: session.snapshot,
                        });
                    }
                    Err(message) => {
                        let _ = events.send(ObsEvent::ConnectFailed { epoch, message });
                    }
                }
            }
            _ = poll.tick() => {
                let outcome = match client.as_ref() {
                    Some(current) => {
                        Some(snapshot_with_audio(current, &mut audio_names, false).await)
                    }
                    None => None,
                };
                if let Some(result) = outcome
                    && !send_snapshot(&events, epoch, result)
                {
                    client = None;
                    audio_names.clear();
                }
            }
        }
    }
}

fn send_audio_result(
    events: &mpsc::UnboundedSender<ObsEvent>,
    epoch: u64,
    outcome: Option<Result<AudioSource, String>>,
    client: &mut Option<Client>,
) -> bool {
    match outcome {
        Some(Ok(source)) => {
            let _ = events.send(ObsEvent::AudioLevel { epoch, source });
            true
        }
        Some(Err(message)) if connection_died(&message) => {
            *client = None;
            let _ = events.send(ObsEvent::Lost { epoch, message });
            false
        }
        Some(Err(message)) => {
            let _ = events.send(ObsEvent::RequestFailed { epoch, message });
            true
        }
        None => {
            let _ = events.send(ObsEvent::RequestFailed {
                epoch,
                message: "Not connected".to_string(),
            });
            true
        }
    }
}

fn send_snapshot(
    events: &mpsc::UnboundedSender<ObsEvent>,
    epoch: u64,
    result: Result<SceneSnapshot, String>,
) -> bool {
    match result {
        Ok(snapshot) => {
            let _ = events.send(ObsEvent::Snapshot { epoch, snapshot });
            true
        }
        Err(message) => {
            let _ = events.send(ObsEvent::Lost { epoch, message });
            false
        }
    }
}

fn connection_died(message: &str) -> bool {
    !message.contains("API error")
}

async fn open_session(creds: &Credentials) -> Result<ConnectedSession, String> {
    let password = (!creds.password.is_empty()).then_some(creds.password.as_str());
    let client = Client::connect_with_config(ConnectConfig {
        host: creds.host.as_str(),
        port: creds.port,
        dangerous: None,
        password,
        event_subscriptions: Some(EventSubscription::NONE),
        broadcast_capacity: DEFAULT_BROADCAST_CAPACITY,
        connect_timeout: CONNECT_TIMEOUT,
    })
    .await
    .map_err(|err| friendly_connect_error(flatten_error(&err)))?;
    let mut snapshot = load_snapshot(&client).await?;
    snapshot.audio = discover_audio(&client).await?;
    Ok(ConnectedSession { client, snapshot })
}

async fn load_snapshot(client: &Client) -> Result<SceneSnapshot, String> {
    let list = client
        .scenes()
        .list()
        .await
        .map_err(|err| flatten_error(&err))?;
    let current = list.current_program_scene.map(|scene| scene.name);
    let scenes = order_scenes(
        list.scenes
            .into_iter()
            .map(|scene| SceneEntry {
                name: scene.id.name,
                index: scene.index,
            })
            .collect(),
    );
    Ok(SceneSnapshot {
        scenes,
        current,
        audio: Vec::new(),
    })
}

async fn snapshot_with_audio(
    client: &Client,
    audio_names: &mut Vec<String>,
    discover: bool,
) -> Result<SceneSnapshot, String> {
    let mut snapshot = load_snapshot(client).await?;
    snapshot.audio = if discover {
        let audio = discover_audio(client).await?;
        *audio_names = audio.iter().map(|source| source.name.clone()).collect();
        audio
    } else {
        sample_audio(client, audio_names).await?
    };
    Ok(snapshot)
}

async fn discover_audio(client: &Client) -> Result<Vec<AudioSource>, String> {
    let inputs = client
        .inputs()
        .list(None)
        .await
        .map_err(|err| flatten_error(&err))?;
    let mut sources = Vec::new();
    for input in inputs {
        match read_audio(client, &input.id.name).await {
            Ok(source) => sources.push(source),
            Err(message) if !connection_died(&message) => {}
            Err(message) => return Err(message),
        }
    }
    sources.sort_by_key(|source| source.name.to_lowercase());
    Ok(sources)
}

async fn sample_audio(
    client: &Client,
    names: &mut Vec<String>,
) -> Result<Vec<AudioSource>, String> {
    let mut sources = Vec::new();
    let mut kept = Vec::new();
    for name in names.iter() {
        match read_audio(client, name).await {
            Ok(source) => {
                kept.push(source.name.clone());
                sources.push(source);
            }
            Err(message) if !connection_died(&message) => {}
            Err(message) => return Err(message),
        }
    }
    *names = kept;
    Ok(sources)
}

async fn read_audio(client: &Client, name: &str) -> Result<AudioSource, String> {
    let id = InputId::Name(name);
    let volume = client
        .inputs()
        .volume(id)
        .await
        .map_err(|err| flatten_error(&err))?;
    let muted = client
        .inputs()
        .muted(id)
        .await
        .map_err(|err| flatten_error(&err))?;
    Ok(AudioSource {
        name: name.to_string(),
        mul: volume.mul,
        muted,
    })
}

async fn change_volume(client: &Client, name: &str, mul: f32) -> Result<AudioSource, String> {
    client
        .inputs()
        .set_volume(InputId::Name(name), Volume::Mul(mul))
        .await
        .map_err(|err| flatten_error(&err))?;
    read_audio(client, name).await
}

async fn change_mute(client: &Client, name: &str) -> Result<AudioSource, String> {
    client
        .inputs()
        .toggle_mute(InputId::Name(name))
        .await
        .map_err(|err| flatten_error(&err))?;
    read_audio(client, name).await
}

async fn set_scene(client: &Client, name: &str) -> Result<(), String> {
    client
        .scenes()
        .set_current_program_scene(SceneId::Name(name))
        .await
        .map_err(|err| flatten_error(&err))
}

fn flatten_error(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let part = cause.to_string();
        if !message.contains(&part) {
            message.push_str(": ");
            message.push_str(&part);
        }
        source = cause.source();
    }
    message.replace(['\n', '\r'], " ")
}

fn friendly_connect_error(message: String) -> String {
    if message.to_lowercase().contains("timeout") {
        "Timed out. Check the host, port, and that OBS WebSocket is enabled.".to_string()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::crossterm::event::KeyEvent;

    fn test_app() -> App {
        App::new(
            "127.0.0.1".to_string(),
            "4455".to_string(),
            String::new(),
            Focus::Action,
        )
    }

    #[test]
    fn volume_steps_stay_between_silence_and_unity() {
        assert_eq!(step_volume(0.0, -1), 0.0);
        assert_eq!(step_volume(1.0, 1), 1.0);
        assert!((step_volume(0.5, 1) - 0.55).abs() < 0.001);
        assert!((step_volume(0.5, -1) - 0.45).abs() < 0.001);
    }

    #[test]
    fn volume_bar_tracks_the_level() {
        assert_eq!(volume_bar(0.0, 4), "[----]");
        assert_eq!(volume_bar(0.5, 4), "[==--]");
        assert_eq!(volume_bar(1.0, 4), "[====]");
        assert_eq!(volume_percent(0.5), 50);
    }

    #[test]
    fn volume_changes_only_while_connected() {
        let mut app = test_app();
        app.focus = Focus::Audio;
        app.audio.push(AudioSource {
            name: "Mic".to_string(),
            mul: 0.5,
            muted: false,
        });
        app.audio_state.select(Some(0));
        let (tx, rx) = mpsc::unbounded_channel();

        app.nudge_volume(1, &tx);
        assert!(rx.is_empty());
        assert!((app.audio[0].mul - 0.5).abs() < 0.001);

        app.conn = ConnState::Connected;
        app.nudge_volume(1, &tx);
        assert!((app.audio[0].mul - 0.55).abs() < 0.001);
        app.toggle_selected_mute(&tx);
        assert!(app.audio[0].muted);
        assert_eq!(rx.len(), 2);
    }

    fn press(app: &mut App, code: KeyCode) -> bool {
        let (tx, _rx) = mpsc::unbounded_channel();
        handle_input(
            app,
            &tx,
            Event::Key(KeyEvent::new(code, KeyModifiers::empty())),
        )
    }

    #[test]
    fn vim_motion_stays_in_the_panel_until_insert() {
        let mut app = test_app();
        app.focus = Focus::Host;
        assert!(!press(&mut app, KeyCode::Char('j')));
        assert_eq!(app.focus, Focus::Port);
        assert!(!app.editing);

        assert!(!press(&mut app, KeyCode::Char('i')));
        assert!(app.editing);
        let before = app.port.value.clone();
        assert!(!press(&mut app, KeyCode::Char('9')));
        assert_eq!(app.port.value, format!("{before}9"));

        assert!(!press(&mut app, KeyCode::Esc));
        assert!(!app.editing);
        assert_eq!(app.focus, Focus::Port);
        assert!(!press(&mut app, KeyCode::Char('k')));
        assert_eq!(app.focus, Focus::Host);

        assert!(!press(&mut app, KeyCode::Char('2')));
        assert_eq!(app.focus, Focus::Scenes);
        assert!(!app.editing);

        app.focus = Focus::Action;
        assert!(!press(&mut app, KeyCode::Char('j')));
        assert_eq!(app.focus, Focus::Action);
        assert!(!press(&mut app, KeyCode::Char('g')));
        assert!(!press(&mut app, KeyCode::Char('g')));
        assert_eq!(app.focus, Focus::Host);
        assert!(!press(&mut app, KeyCode::Char('G')));
        assert_eq!(app.focus, Focus::Action);

        app.focus = Focus::Host;
        assert!(!press(&mut app, KeyCode::Char('i')));
        assert!(!press(&mut app, KeyCode::Char('q')));
        assert!(app.editing);
        assert!(app.host.value.ends_with('q'));

        assert!(!press(&mut app, KeyCode::Esc));
        assert!(!app.editing);
        assert!(press(&mut app, KeyCode::Char('q')));
    }

    #[test]
    fn insert_selects_the_highlighted_scene() {
        let mut app = test_app();
        app.focus = Focus::Scenes;
        app.scenes.push(SceneEntry {
            name: "Gaming".to_string(),
            index: 1,
        });
        app.select(0);
        assert!(!press(&mut app, KeyCode::Char('i')));
        assert!(matches!(app.notice, Notice::Err(_)));
    }

    #[test]
    fn horizontal_motion_changes_audio_volume() {
        let mut app = test_app();
        app.conn = ConnState::Connected;
        app.focus = Focus::Audio;
        app.audio.push(AudioSource {
            name: "Mic".to_string(),
            mul: 0.5,
            muted: false,
        });
        app.select_audio(0);
        assert!(!press(&mut app, KeyCode::Char('h')));
        assert!((app.audio[0].mul - 0.45).abs() < 0.001);
        assert!(!press(&mut app, KeyCode::Char('l')));
        assert!((app.audio[0].mul - 0.5).abs() < 0.001);
        assert!(!press(&mut app, KeyCode::Char('i')));
        assert!(app.audio[0].muted);
    }

    #[test]
    fn number_keys_focus_each_window() {
        let mut app = test_app();
        app.focus_window(2);
        assert_eq!(app.focus, Focus::Scenes);
        app.focus_window(3);
        assert_eq!(app.focus, Focus::Audio);
        app.focus_window(1);
        assert_eq!(app.focus, Focus::Host);
    }

    #[test]
    fn logo_rows_line_up() {
        let rows: Vec<String> = (0..5)
            .map(|row| format!("{}  {}", word_row("SCENE", row), word_row("SHIFT", row)))
            .collect();
        let width = rows[0].chars().count();
        assert!(width <= 64);
        assert!(rows.iter().all(|row| row.chars().count() == width));
        assert!(rows.iter().all(|row| row.is_ascii()));
        for letter in ["S", "C", "E", "N", "H", "I", "F", "T"] {
            let glyph = glyph(letter.chars().next().unwrap());
            assert!(glyph.iter().all(|row| row.chars().count() == 5));
        }
    }

    #[test]
    fn field_edits_by_character() {
        let mut field = Field::new("é");
        assert_eq!(field.cursor, 1);
        field.backspace();
        assert_eq!(field.value, "");

        field.insert('a', 10);
        field.insert('b', 10);
        field.move_cursor(-1);
        field.insert('c', 10);
        assert_eq!(field.value, "acb");
        assert_eq!(field.cursor, 2);
        field.delete_char();
        assert_eq!(field.value, "ac");
    }

    #[test]
    fn field_rejects_past_the_max_length() {
        let mut field = Field::new("");
        field.insert('1', 2);
        field.insert('2', 2);
        field.insert('3', 2);
        assert_eq!(field.value, "12");
    }

    #[test]
    fn port_must_be_between_1_and_65535() {
        assert_eq!(parse_port("4455"), Ok(4455));
        assert_eq!(parse_port(" 80 "), Ok(80));
        assert!(parse_port("0").is_err());
        assert!(parse_port("").is_err());
        assert!(parse_port("99999").is_err());
        assert!(parse_port("abc").is_err());
    }

    #[test]
    fn scenes_are_ordered_like_the_obs_dock() {
        let ordered = order_scenes(vec![
            SceneEntry {
                name: "Bottom".to_string(),
                index: 0,
            },
            SceneEntry {
                name: "Top".to_string(),
                index: 2,
            },
            SceneEntry {
                name: "Middle".to_string(),
                index: 1,
            },
        ]);
        let names: Vec<_> = ordered.into_iter().map(|scene| scene.name).collect();
        assert_eq!(names, ["Top", "Middle", "Bottom"]);
    }

    #[test]
    fn button_follows_the_connection_and_edited_fields() {
        let mut app = test_app();
        assert_eq!(app.action_label(), "Connect");

        app.conn = ConnState::Connecting;
        assert_eq!(app.action_label(), "Cancel");

        app.conn = ConnState::Connected;
        app.active = Some(Credentials {
            host: "127.0.0.1".to_string(),
            port: 4455,
            password: String::new(),
        });
        assert_eq!(app.action_label(), "Disconnect");

        app.host.value = "10.0.0.8".to_string();
        assert_eq!(app.action_label(), "Reconnect");
    }

    #[test]
    fn invalid_port_does_not_start_a_connection() {
        let mut app = test_app();
        app.port.value = "nope".to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        app.connect(&tx);
        assert_eq!(
            app.conn,
            ConnState::Failed("Port must be between 1 and 65535".to_string())
        );
        assert!(rx.is_empty());
    }

    #[test]
    fn bad_input_keeps_an_existing_connection() {
        let mut app = test_app();
        app.conn = ConnState::Connected;
        app.active = Some(Credentials {
            host: "127.0.0.1".to_string(),
            port: 4455,
            password: String::new(),
        });
        app.port.value.clear();
        let (tx, rx) = mpsc::unbounded_channel();
        app.connect(&tx);
        assert_eq!(app.conn, ConnState::Connected);
        assert!(rx.is_empty());
        assert!(matches!(app.notice, Notice::Err(_)));
    }
}
