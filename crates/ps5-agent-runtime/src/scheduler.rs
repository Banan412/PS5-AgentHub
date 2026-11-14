use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc,
};
use ps5_agent_core::{
    AgentDefinition, AgentError, ApiMode, InferenceMessage, InferenceProvider, InferenceRequest,
    Result, ScheduleKind, ScheduleRecord, ThinkingLevel,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const MAX_CRON_SEARCH_MINUTES: i64 = 527_040; // 366 days * 24h * 60m
const MIN_INTERVAL_SECS: u64 = 10;
const MAX_INTERVAL_SECS: u64 = 31_536_000 * 10; // 10 years
const MAX_HEURISTIC_SCAN_TOKENS: usize = 256;

/// Returns non-negative seconds remaining until `next_run_at` from `now`
/// (`Some(0)` if `next_run_at <= now`, avoiding negative/wrapped `u64` countdowns).
pub fn seconds_until_next_run(
    next_run_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<u64> {
    let next = next_run_at?;
    if next <= now {
        return Some(0);
    }
    Some(u64::try_from((next - now).num_seconds()).unwrap_or(0))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateScheduleInput {
    pub title: String,
    pub prompt: String,
    #[serde(default)]
    pub target_agent_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    pub kind: ScheduleKind,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub daily_time: Option<String>,
    #[serde(default)]
    pub cron_expr: Option<String>,
    #[serde(default)]
    pub timezone_offset_mins: i32,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateScheduleInput {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub target_agent_id: Option<String>,
    #[serde(default)]
    pub kind: Option<ScheduleKind>,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub daily_time: Option<String>,
    #[serde(default)]
    pub cron_expr: Option<String>,
    #[serde(default)]
    pub timezone_offset_mins: Option<i32>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiScheduleDraft {
    pub title: String,
    pub prompt: String,
    pub kind: ScheduleKind,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub daily_time: Option<String>,
    #[serde(default)]
    pub cron_expr: Option<String>,
    #[serde(default)]
    pub target_agent_id: Option<String>,
    #[serde(default)]
    pub timezone_offset_mins: i32,
    pub explanation: String,
    pub source: String,
}

#[derive(Debug, Clone)]
struct ParsedCron {
    minutes: [bool; 60],
    hours: [bool; 24],
    doms: [bool; 32],
    months: [bool; 13],
    dows: [bool; 7],
    dom_wildcard: bool,
    dow_wildcard: bool,
}

impl ParsedCron {
    fn parse(expr: &str) -> Result<Self> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(AgentError::InvalidInput(format!(
                "cron expression must have 5 fields (min hour dom month dow), got {}",
                fields.len()
            )));
        }

        let mut minutes = [false; 60];
        let mut hours = [false; 24];
        let mut doms = [false; 32];
        let mut months = [false; 13];
        let mut dows = [false; 7];

        parse_cron_field(fields[0], 0, 59, false, &mut minutes)?;
        parse_cron_field(fields[1], 0, 23, false, &mut hours)?;
        parse_cron_field(fields[2], 1, 31, false, &mut doms)?;
        parse_cron_field(fields[3], 1, 12, false, &mut months)?;
        parse_cron_field(fields[4], 0, 7, true, &mut dows)?;

        Ok(Self {
            minutes,
            hours,
            doms,
            months,
            dows,
            dom_wildcard: fields[2].trim() == "*",
            dow_wildcard: fields[4].trim() == "*",
        })
    }

    fn matches_date(&self, date: chrono::NaiveDate) -> bool {
        let month = date.month() as usize;
        if month >= self.months.len() || !self.months[month] {
            return false;
        }
        let dom = date.day() as usize;
        let dom_match = dom < self.doms.len() && self.doms[dom];
        let dow = date.weekday().num_days_from_sunday() as usize;
        let dow_match = dow < self.dows.len() && self.dows[dow];

        if !self.dom_wildcard && !self.dow_wildcard {
            dom_match || dow_match
        } else {
            dom_match && dow_match
        }
    }
}

fn parse_cron_field(
    raw: &str,
    min: u32,
    max: u32,
    wrap_sunday: bool,
    out: &mut [bool],
) -> Result<()> {
    let field = raw.trim();
    if field.is_empty() {
        return Err(AgentError::InvalidInput("empty cron field".into()));
    }

    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(AgentError::InvalidInput(format!(
                "invalid empty list item in cron field '{field}'"
            )));
        }
        let (range_part, step) = if let Some((left, right)) = part.split_once('/') {
            let step: u32 = right.trim().parse().map_err(|_| {
                AgentError::InvalidInput(format!("invalid cron step '{right}' in '{field}'"))
            })?;
            if step == 0 {
                return Err(AgentError::InvalidInput(format!(
                    "cron step must be > 0 in '{field}'"
                )));
            }
            (left.trim(), step)
        } else {
            (part, 1)
        };

        let (start, end) = if range_part == "*" {
            (min, max)
        } else if let Some((a_str, b_str)) = range_part.split_once('-') {
            let a: u32 = a_str.trim().parse().map_err(|_| {
                AgentError::InvalidInput(format!("invalid cron range start '{a_str}' in '{field}'"))
            })?;
            let b: u32 = b_str.trim().parse().map_err(|_| {
                AgentError::InvalidInput(format!("invalid cron range end '{b_str}' in '{field}'"))
            })?;
            if a < min || b > max || a > b {
                return Err(AgentError::InvalidInput(format!(
                    "cron range {a}-{b} out of bounds ({min}..={max}) in '{field}'"
                )));
            }
            (a, b)
        } else {
            let v: u32 = range_part.parse().map_err(|_| {
                AgentError::InvalidInput(format!("invalid cron value '{range_part}' in '{field}'"))
            })?;
            if v < min || v > max {
                return Err(AgentError::InvalidInput(format!(
                    "cron value {v} out of bounds ({min}..={max}) in '{field}'"
                )));
            }
            if step == 1 {
                (v, v)
            } else {
                (v, max)
            }
        };

        let mut v = start;
        while v <= end {
            let idx = if wrap_sunday && v == 7 { 0 } else { v as usize };
            if idx < out.len() {
                out[idx] = true;
            }
            v = match v.checked_add(step) {
                Some(next) => next,
                None => break,
            };
        }
    }

    if !out.iter().any(|&set| set) {
        return Err(AgentError::InvalidInput(format!(
            "cron field '{field}' matched no valid values"
        )));
    }
    Ok(())
}

fn fixed_offset_from_mins(timezone_offset_mins: i32) -> Result<FixedOffset> {
    let offset_secs = timezone_offset_mins
        .checked_mul(60)
        .ok_or_else(|| AgentError::InvalidInput("timezone_offset_mins out of range".into()))?;
    FixedOffset::east_opt(offset_secs).ok_or_else(|| {
        AgentError::InvalidInput(format!(
            "timezone_offset_mins ({timezone_offset_mins}) must be within -1439..=1439"
        ))
    })
}

fn parse_daily_time(raw: &str) -> Result<NaiveTime> {
    let trimmed = raw.trim();
    let parts: Vec<&str> = trimmed.split(':').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return Err(AgentError::InvalidInput(format!(
            "daily_time '{trimmed}' must be HH:MM or HH:MM:SS"
        )));
    }
    let hour: u32 = parts[0].parse().map_err(|_| {
        AgentError::InvalidInput(format!("invalid hour in daily_time '{trimmed}'"))
    })?;
    let minute: u32 = parts[1].parse().map_err(|_| {
        AgentError::InvalidInput(format!("invalid minute in daily_time '{trimmed}'"))
    })?;
    let second: u32 = if parts.len() == 3 {
        parts[2].parse().map_err(|_| {
            AgentError::InvalidInput(format!("invalid second in daily_time '{trimmed}'"))
        })?
    } else {
        0
    };
    NaiveTime::from_hms_opt(hour, minute, second).ok_or_else(|| {
        AgentError::InvalidInput(format!("invalid time of day in daily_time '{trimmed}'"))
    })
}

pub fn validate_schedule_fields(schedule: &ScheduleRecord) -> Result<()> {
    let _ = fixed_offset_from_mins(schedule.timezone_offset_mins)?;
    match schedule.kind {
        ScheduleKind::Once => Ok(()),
        ScheduleKind::Interval => {
            let secs = schedule.interval_secs.ok_or_else(|| {
                AgentError::InvalidInput("interval schedule requires interval_secs".into())
            })?;
            if secs < MIN_INTERVAL_SECS {
                return Err(AgentError::InvalidInput(format!(
                    "interval_secs must be at least {MIN_INTERVAL_SECS} seconds"
                )));
            }
            if secs > MAX_INTERVAL_SECS {
                return Err(AgentError::InvalidInput(format!(
                    "interval_secs exceeds maximum ({MAX_INTERVAL_SECS})"
                )));
            }
            Ok(())
        }
        ScheduleKind::Daily => {
            let raw = schedule
                .daily_time
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AgentError::InvalidInput("daily schedule requires daily_time".into())
                })?;
            let _ = parse_daily_time(raw)?;
            Ok(())
        }
        ScheduleKind::Cron => {
            let raw = schedule
                .cron_expr
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AgentError::InvalidInput("cron schedule requires cron_expr".into())
                })?;
            let _ = ParsedCron::parse(raw)?;
            Ok(())
        }
    }
}

pub fn compute_next_run_at(
    schedule: &ScheduleRecord,
    from: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    if !schedule.enabled {
        return Ok(None);
    }

    let tz = fixed_offset_from_mins(schedule.timezone_offset_mins)?;

    match schedule.kind {
        ScheduleKind::Once => {
            if schedule.run_count > 0 {
                return Ok(None);
            }
            Ok(Some(schedule.run_at.unwrap_or(from)))
        }
        ScheduleKind::Interval => {
            validate_schedule_fields(schedule)?;
            let secs = schedule.interval_secs.unwrap_or(MIN_INTERVAL_SECS);
            let secs_i64 = i64::try_from(secs)
                .map_err(|_| AgentError::InvalidInput("interval_secs out of range".into()))?;
            let delta = Duration::try_seconds(secs_i64)
                .ok_or_else(|| AgentError::InvalidInput("interval_secs duration overflow".into()))?;
            let next = from.checked_add_signed(delta).ok_or_else(|| {
                AgentError::InvalidInput("timestamp overflow computing next interval run".into())
            })?;
            Ok(Some(next))
        }
        ScheduleKind::Daily => {
            let raw = schedule
                .daily_time
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AgentError::InvalidInput("daily schedule requires daily_time".into())
                })?;
            let time = parse_daily_time(raw)?;
            let local_from = from.with_timezone(&tz);
            let today = local_from.date_naive();
            let today_local = NaiveDateTime::new(today, time);
            let today_utc = tz
                .from_local_datetime(&today_local)
                .single()
                .ok_or_else(|| AgentError::InvalidInput("invalid local daily datetime".into()))?
                .with_timezone(&Utc);
            if today_utc > from {
                return Ok(Some(today_utc));
            }
            let tomorrow = today
                .succ_opt()
                .ok_or_else(|| AgentError::InvalidInput("date overflow computing next daily run".into()))?;
            let tomorrow_local = NaiveDateTime::new(tomorrow, time);
            let tomorrow_utc = tz
                .from_local_datetime(&tomorrow_local)
                .single()
                .ok_or_else(|| AgentError::InvalidInput("invalid local daily datetime".into()))?
                .with_timezone(&Utc);
            Ok(Some(tomorrow_utc))
        }
        ScheduleKind::Cron => {
            let raw = schedule
                .cron_expr
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AgentError::InvalidInput("cron schedule requires cron_expr".into())
                })?;
            let cron = ParsedCron::parse(raw)?;
            let one_min = Duration::minutes(1);
            let base = from.checked_add_signed(one_min).ok_or_else(|| {
                AgentError::InvalidInput("timestamp overflow computing cron base".into())
            })?;
            let base_local = base.with_timezone(&tz);
            let start_time = NaiveTime::from_hms_opt(base_local.hour(), base_local.minute(), 0)
                .ok_or_else(|| AgentError::InvalidInput("invalid cron start time".into()))?;
            let mut cur = NaiveDateTime::new(base_local.date_naive(), start_time);
            let limit = cur
                .checked_add_signed(Duration::minutes(MAX_CRON_SEARCH_MINUTES))
                .unwrap_or(NaiveDateTime::MAX);

            while cur < limit {
                if !cron.matches_date(cur.date()) {
                    let Some(next_date) = cur.date().succ_opt() else {
                        break;
                    };
                    cur = NaiveDateTime::new(
                        next_date,
                        NaiveTime::from_hms_opt(0, 0, 0).expect("valid midnight"),
                    );
                    continue;
                }
                if !cron.hours[cur.hour() as usize] {
                    let Some(next) = cur.checked_add_signed(Duration::minutes(60 - cur.minute() as i64)) else {
                        break;
                    };
                    cur = next;
                    continue;
                }
                if cron.minutes[cur.minute() as usize] {
                    let local_dt = tz
                        .from_local_datetime(&cur)
                        .single()
                        .ok_or_else(|| {
                            AgentError::InvalidInput("invalid local cron datetime".into())
                        })?;
                    return Ok(Some(local_dt.with_timezone(&Utc)));
                }
                let Some(next) = cur.checked_add_signed(one_min) else {
                    break;
                };
                cur = next;
            }

            Err(AgentError::InvalidInput(
                "cron expression has no matching execution within 366 days".into(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
struct SpanToken<'a> {
    start: usize,
    end: usize,
    raw: &'a str,
    clean_folded: String,
}

fn fold_polish_ascii(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        let mapped = match ch {
            'ą' | 'Ą' => 'a',
            'ć' | 'Ć' => 'c',
            'ę' | 'Ę' => 'e',
            'ł' | 'Ł' => 'l',
            'ń' | 'Ń' => 'n',
            'ó' | 'Ó' => 'o',
            'ś' | 'Ś' => 's',
            'ź' | 'Ź' | 'ż' | 'Ż' => 'z',
            other => {
                for lc in other.to_lowercase() {
                    out.push(lc);
                }
                continue;
            }
        };
        out.push(mapped);
    }
    out
}

#[derive(Default)]
struct LanguageScores {
    polish: u32,
    english: u32,
    german: u32,
    spanish: u32,
    french: u32,
}

fn is_polish_unique_diacritic(ch: char) -> bool {
    matches!(
        ch,
        'ą' | 'Ą' | 'ć' | 'Ć' | 'ę' | 'Ę' | 'ł' | 'Ł' | 'ń' | 'Ń' | 'ś' | 'Ś' | 'ź' | 'Ź' | 'ż'
            | 'Ż'
    )
}

fn is_known_european_accent(ch: char) -> bool {
    is_polish_unique_diacritic(ch)
        || matches!(
            ch,
            'ó' | 'Ó'
                | 'ä'
                | 'Ä'
                | 'ö'
                | 'Ö'
                | 'ü'
                | 'Ü'
                | 'ß'
                | '¿'
                | '¡'
                | 'ñ'
                | 'Ñ'
                | 'á'
                | 'Á'
                | 'é'
                | 'É'
                | 'í'
                | 'Í'
                | 'ú'
                | 'Ú'
                | 'ç'
                | 'Ç'
                | 'œ'
                | 'Œ'
                | 'à'
                | 'À'
                | 'â'
                | 'Â'
                | 'è'
                | 'È'
                | 'ê'
                | 'Ê'
                | 'ë'
                | 'Ë'
                | 'î'
                | 'Î'
                | 'ï'
                | 'Ï'
                | 'ô'
                | 'Ô'
                | 'ù'
                | 'Ù'
                | 'û'
                | 'Û'
        )
}

fn normalize_lower_apostrophe(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch == '’' {
            out.push('\'');
        } else {
            for lc in ch.to_lowercase() {
                out.push(lc);
            }
        }
    }
    out
}

fn extract_language_words(text: &str) -> Vec<(String, String)> {
    text.split(|ch: char| !ch.is_alphanumeric() && ch != '\'' && ch != '’')
        .filter_map(|raw| {
            let trimmed = raw.trim_matches(|c: char| c == '\'' || c == '’');
            if trimmed.is_empty() {
                None
            } else {
                let raw_lower = normalize_lower_apostrophe(trimmed);
                let folded = fold_polish_ascii(&raw_lower);
                Some((raw_lower, folded))
            }
        })
        .collect()
}

fn contains_word(haystack: &[&str], word: &str) -> bool {
    haystack.contains(&word)
}

const POLISH_HIGH_SIGNAL_WORDS: &[&str] = &[
    "czesc",
    "hej",
    "siema",
    "witam",
    "witaj",
    "prosze",
    "zrob",
    "zrobic",
    "zrobiles",
    "sprawdz",
    "sprawdzic",
    "napraw",
    "naprawic",
    "pokaz",
    "otworz",
    "znajdz",
    "szukaj",
    "wyszukaj",
    "wyjasnij",
    "podsumuj",
    "przygotuj",
    "przypomnij",
    "utworz",
    "usun",
    "zmien",
    "dodaj",
    "policz",
    "spojrz",
    "zobacz",
    "slyszysz",
    "slyszusz",
    "slychac",
    "widac",
    "mnie",
    "powiedz",
    "napisz",
    "zapytaj",
    "spytaj",
    "popros",
    "przekaz",
    "zlec",
    "niech",
    "odpisz",
    "odpisac",
    "odpowiedz",
    "rozmawiaj",
    "gadaj",
    "dlaczego",
    "czemu",
    "gdzie",
    "kiedy",
    "czego",
    "jakie",
    "jakiego",
    "ktory",
    "ktora",
    "ktore",
    "czy",
    "jestes",
    "mozesz",
    "mozna",
    "moze",
    "mozemy",
    "bedzie",
    "chce",
    "potrzebuje",
    "potrzeba",
    "dziala",
    "dzialaj",
    "wlacz",
    "wylacz",
    "uruchom",
    "zatrzymaj",
    "odpal",
    "wszystko",
    "wszystkie",
    "kazdy",
    "teraz",
    "jeszcze",
    "tutaj",
    "bardzo",
    "dobrze",
    "swietnie",
    "dzisiaj",
    "dzis",
    "jutro",
    "wczoraj",
    "codziennie",
    "tydzien",
    "miesiac",
    "dobry",
    "dobranoc",
    "dobrego",
    "dziekuje",
    "dzieki",
    "przepraszam",
    "zeby",
    "aby",
    "polsku",
    "plik",
    "pliki",
    "plikow",
    "pamiec",
    "blad",
    "bledy",
    "pomoc",
    "pomocy",
    "pomoz",
    "moj",
    "twoj",
    "tak",
    "komander",
    "dowodca",
    "dowodco",
    "dowodcy",
    "wykonawca",
    "wykonawco",
    "wykonawcy",
    "programista",
    "programisto",
    "programisty",
    "badacz",
    "badacza",
    "analityk",
    "analityka",
    "commanderowi",
    "executorowi",
    "programmerowi",
];

const POLISH_PHRASES: &[(&str, &str)] = &[
    ("dzien", "dobry"),
    ("co", "tam"),
    ("jak", "sie"),
    ("po", "polsku"),
    ("daj", "znac"),
    ("nie", "ma"),
    ("nie", "wiem"),
    ("nie", "dziala"),
    ("czy", "mozesz"),
    ("ze", "ma"),
];

const ENGLISH_HIGH_SIGNAL_WORDS: &[&str] = &[
    "hello",
    "hi",
    "hey",
    "hear",
    "the",
    "is",
    "are",
    "was",
    "were",
    "can",
    "could",
    "would",
    "should",
    "will",
    "do",
    "does",
    "did",
    "you",
    "your",
    "my",
    "our",
    "they",
    "what",
    "how",
    "why",
    "where",
    "when",
    "who",
    "which",
    "please",
    "fix",
    "help",
    "search",
    "find",
    "check",
    "make",
    "build",
    "create",
    "open",
    "show",
    "tell",
    "ask",
    "write",
    "read",
    "run",
    "list",
    "explain",
    "describe",
    "summarize",
    "thanks",
    "thank",
    "sorry",
    "yes",
    "not",
    "and",
    "for",
    "with",
    "from",
    "this",
    "that",
    "there",
    "here",
    "have",
    "has",
    "need",
    "want",
    "work",
    "works",
    "working",
    "good",
    "morning",
    "evening",
    "reply",
    "answer",
    "respond",
    "speak",
    "english",
    "i'm",
    "you're",
    "what's",
    "how's",
    "it's",
    "that's",
    "don't",
    "doesn't",
    "didn't",
    "can't",
    "won't",
    "isn't",
    "aren't",
];

const GERMAN_HIGH_SIGNAL_WORDS: &[&str] = &[
    "bitte",
    "danke",
    "entschuldigung",
    "tschuss",
    "warum",
    "nicht",
    "keine",
    "naturlich",
    "deutsch",
    "servus",
    "moin",
    "hilfe",
];

const GERMAN_ACCENTED_WORDS: &[&str] = &[
    "tschüss",
    "natürlich",
    "für",
    "können",
    "möchte",
    "grüße",
    "straße",
];

const GERMAN_PHRASES: &[(&str, &str)] = &[
    ("guten", "tag"),
    ("guten", "morgen"),
    ("guten", "abend"),
    ("wie", "geht"),
    ("wie", "gehts"),
    ("wie", "geht's"),
    ("ich", "brauche"),
    ("kannst", "du"),
    ("hilf", "mir"),
];

const SPANISH_HIGH_SIGNAL_WORDS: &[&str] = &[
    "hola",
    "gracias",
    "buenos",
    "buenas",
    "necesito",
    "puedes",
    "ayuda",
    "espanol",
    "saludos",
];

const SPANISH_ACCENTED_WORDS: &[&str] = &[
    "qué",
    "cómo",
    "dónde",
    "estás",
    "porqué",
    "adiós",
    "español",
];

const SPANISH_PHRASES: &[(&str, &str)] = &[
    ("por", "favor"),
    ("buenos", "dias"),
    ("buenas", "tardes"),
    ("buenas", "noches"),
];

const FRENCH_HIGH_SIGNAL_WORDS: &[&str] = &[
    "bonjour",
    "bonsoir",
    "salut",
    "merci",
    "pouvez",
    "pourquoi",
    "francais",
    "oui",
    "s'il",
    "c'est",
    "j'ai",
    "peux",
];

const FRENCH_ACCENTED_WORDS: &[&str] = &["où", "ça", "très", "être", "français", "voilà"];

const FRENCH_PHRASES: &[(&str, &str)] = &[
    ("s'il", "vous"),
    ("sil", "vous"),
    ("c'est", "quoi"),
    ("en", "francais"),
];

const AMBIGUOUS_UNIVERSAL_WORDS: &[&str] = &[
    "ok", "okay", "k", "kk", "hm", "hmm", "mhm", "aha", "uh", "um", "eh", "oh", "ah", "lol", "gg",
    "thx", "brb", "np", "xd",
];

/// Detects the primary natural language of the latest user message text alone,
/// ignoring prior history/memory so short English prompts do not inherit Polish context.
///
/// Returns one of `"Polish"`, `"English"`, `"German"`, `"Spanish"`, `"French"`, or `"auto"`.
pub(crate) fn detect_user_language_name(text: &str) -> &'static str {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "auto";
    }

    let mut scores = LanguageScores::default();
    let mut has_o_acute = false;
    let mut has_other_non_latin_alpha = false;

    for ch in trimmed.chars() {
        if is_polish_unique_diacritic(ch) {
            scores.polish = scores.polish.saturating_add(3);
        } else if matches!(ch, 'ó' | 'Ó') {
            has_o_acute = true;
        } else if ch == 'ß' {
            scores.german = scores.german.saturating_add(3);
        } else if matches!(ch, 'ä' | 'Ä' | 'ö' | 'Ö' | 'ü' | 'Ü') {
            scores.german = scores.german.saturating_add(2);
        } else if matches!(ch, '¿' | '¡' | 'ñ' | 'Ñ') {
            scores.spanish = scores.spanish.saturating_add(3);
        } else if matches!(ch, 'ç' | 'Ç' | 'œ' | 'Œ') {
            scores.french = scores.french.saturating_add(2);
        } else if ch.is_alphabetic() && !ch.is_ascii() && !is_known_european_accent(ch) {
            has_other_non_latin_alpha = true;
        }
    }

    if has_o_acute {
        scores.polish = scores.polish.saturating_add(1);
    }

    let words = extract_language_words(trimmed);
    let has_any_alpha = words
        .iter()
        .any(|(_, folded)| folded.chars().any(|c| c.is_alphabetic()));
    if !has_any_alpha {
        return "auto";
    }

    for (raw_lower, folded) in &words {
        let f = folded.as_str();
        let r = raw_lower.as_str();

        if contains_word(POLISH_HIGH_SIGNAL_WORDS, f) {
            scores.polish = scores.polish.saturating_add(2);
        }
        if contains_word(ENGLISH_HIGH_SIGNAL_WORDS, f) {
            scores.english = scores.english.saturating_add(2);
        } else if f == "hallo" || f == "commander" {
            // Treat "hallo" without German words/diacritics as English ("hello" typo),
            // and keep "commander" lighter than strong non-English words.
            scores.english = scores.english.saturating_add(1);
        } else if f == "me" {
            scores.english = scores.english.saturating_add(1);
        }

        if contains_word(GERMAN_HIGH_SIGNAL_WORDS, f)
            || contains_word(GERMAN_ACCENTED_WORDS, r)
        {
            scores.german = scores.german.saturating_add(2);
        }
        if contains_word(SPANISH_HIGH_SIGNAL_WORDS, f)
            || contains_word(SPANISH_ACCENTED_WORDS, r)
        {
            scores.spanish = scores.spanish.saturating_add(2);
        }
        if contains_word(FRENCH_HIGH_SIGNAL_WORDS, f)
            || contains_word(FRENCH_ACCENTED_WORDS, r)
        {
            scores.french = scores.french.saturating_add(2);
        }
    }

    if scores.german == 0 && words.iter().any(|(_, f)| f == "nie") {
        scores.polish = scores.polish.saturating_add(1);
    }

    for pair in words.windows(2) {
        let a = pair[0].1.as_str();
        let b = pair[1].1.as_str();
        if POLISH_PHRASES.contains(&(a, b)) {
            scores.polish = scores.polish.saturating_add(3);
        }
        if GERMAN_PHRASES.contains(&(a, b)) {
            scores.german = scores.german.saturating_add(3);
        }
        if SPANISH_PHRASES.contains(&(a, b)) {
            scores.spanish = scores.spanish.saturating_add(3);
        }
        if FRENCH_PHRASES.contains(&(a, b)) {
            scores.french = scores.french.saturating_add(3);
        }
    }

    let best = [
        ("Polish", scores.polish),
        ("German", scores.german),
        ("Spanish", scores.spanish),
        ("French", scores.french),
        ("English", scores.english),
    ]
    .into_iter()
    .max_by_key(|(_, score)| *score);

    if let Some((lang, top_score)) = best {
        if top_score > 0 {
            return lang;
        }
    }

    if has_other_non_latin_alpha {
        return "auto";
    }

    let all_ambiguous_or_non_alpha = words.iter().all(|(_, f)| {
        contains_word(AMBIGUOUS_UNIVERSAL_WORDS, f) || !f.chars().any(|c| c.is_alphabetic())
    });
    if all_ambiguous_or_non_alpha {
        return "auto";
    }

    let single_letter_only = words.len() == 1
        && words[0]
            .1
            .chars()
            .filter(|c| c.is_alphabetic())
            .count()
            <= 1;
    if single_letter_only {
        return "auto";
    }

    if words
        .iter()
        .any(|(_, f)| f.is_ascii() && f.chars().any(|c| c.is_ascii_alphabetic()))
    {
        return "English";
    }

    "auto"
}

/// Builds an explicit per-turn response-language directive for the latest user message,
/// preventing prior Polish history/memory context from overriding short English turns.
pub(crate) fn language_response_directive(text: &str) -> &'static str {
    match detect_user_language_name(text) {
        "Polish" => {
            "Respond strictly in Polish. Ignore the language of earlier messages or memory entries if the user's latest message is in Polish."
        }
        "English" => {
            "Respond strictly in English. Ignore the language of earlier messages or memory entries if the user's latest message is in English."
        }
        "German" => {
            "Respond strictly in German. Ignore the language of earlier messages or memory entries if the user's latest message is in German."
        }
        "Spanish" => {
            "Respond strictly in Spanish. Ignore the language of earlier messages or memory entries if the user's latest message is in Spanish."
        }
        "French" => {
            "Respond strictly in French. Ignore the language of earlier messages or memory entries if the user's latest message is in French."
        }
        _ => "Reply in the exact same language as the user's latest message above.",
    }
}

fn trim_edge_punct(s: &str) -> &str {
    s.trim_matches(|c: char| matches!(c, ',' | '.' | ';' | '!' | '?' | '"' | '\'' | '(' | ')' | '[' | ']' | '`'))
}

fn tokenize_with_spans(input: &str) -> Vec<SpanToken<'_>> {
    let mut tokens = Vec::new();
    let mut in_tok = false;
    let mut tok_start = 0usize;

    for (idx, ch) in input.char_indices() {
        if ch.is_whitespace() {
            if in_tok {
                let raw = &input[tok_start..idx];
                tokens.push(SpanToken {
                    start: tok_start,
                    end: idx,
                    raw,
                    clean_folded: fold_polish_ascii(trim_edge_punct(raw)),
                });
                in_tok = false;
            }
        } else if !in_tok {
            tok_start = idx;
            in_tok = true;
        }
    }
    if in_tok {
        let raw = &input[tok_start..];
        tokens.push(SpanToken {
            start: tok_start,
            end: input.len(),
            raw,
            clean_folded: fold_polish_ascii(trim_edge_punct(raw)),
        });
    }
    tokens
}

fn parse_hhmm_token(token: &str) -> Option<(u32, u32, String)> {
    let trimmed = token.trim_matches(|c: char| !c.is_ascii_digit() && c != ':');
    let (h_str, m_str) = trimmed.split_once(':')?;
    if h_str.is_empty() || m_str.is_empty() || m_str.contains(':') {
        return None;
    }
    let h: u32 = h_str.parse().ok()?;
    let m: u32 = m_str.parse().ok()?;
    if h < 24 && m < 60 {
        Some((h, m, format!("{h:02}:{m:02}")))
    } else {
        None
    }
}

fn unit_multiplier(unit: &str) -> Option<u64> {
    match unit {
        "s" | "sec" | "secs" | "second" | "seconds" | "sek" | "sekunda" | "sekundy" | "sekund"
        | "sekunde" => Some(1),
        "m" | "min" | "mins" | "minute" | "minutes" | "minuta" | "minuty" | "minut" => Some(60),
        "h" | "hr" | "hrs" | "hour" | "hours" | "godz" | "godzina" | "godziny" | "godzin"
        | "godzine" => Some(3600),
        _ => None,
    }
}

fn parse_number_and_unit_single(tok: &str) -> Option<(u64, u64)> {
    let digit_len = tok.bytes().take_while(|b| b.is_ascii_digit()).count();
    if digit_len == 0 || digit_len == tok.len() {
        return None;
    }
    let n: u64 = tok[..digit_len].parse().ok()?;
    let unit = &tok[digit_len..];
    unit_multiplier(unit).map(|mult| (n, mult))
}

fn detect_target_agent(tokens: &[SpanToken<'_>], preferred_agent: Option<&str>) -> Option<String> {
    for tok in tokens {
        let bare = tok.clean_folded.trim_start_matches('@');
        let matched = match bare {
            "commander" | "commandera" | "commanderowi" | "commanderem" | "commanderze"
            | "dowodca" | "dowodcy" | "dowodce" => Some("commander"),
            "executor" | "executora" | "executorowi" | "executorem" | "executorze"
            | "wykonawca" => Some("executor"),
            "programmer" | "programmera" | "programmerowi" | "programista" | "programisty"
            | "programiscie" => Some("programmer"),
            "researcher" | "researchera" | "researcherowi" | "badacz" | "badacza" => {
                Some("researcher")
            }
            "analyst" | "analysta" | "analystowi" | "analityk" | "analityka" => Some("analyst"),
            _ => None,
        };
        if let Some(id) = matched {
            return Some(id.to_string());
        }
    }
    preferred_agent
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn remove_span_and_clean(raw: &str, span_start: usize, span_end: usize) -> String {
    let before = raw[..span_start].trim();
    let after = raw[span_end..].trim();
    let combined = match (before.is_empty(), after.is_empty()) {
        (true, true) => String::new(),
        (true, false) => after.to_string(),
        (false, true) => before.to_string(),
        (false, false) => format!("{before} {after}"),
    };
    let trimmed = combined
        .trim()
        .trim_start_matches(|c: char| matches!(c, ':' | '-' | ',' | ';'))
        .trim();
    let folded_prefix = fold_polish_ascii(trimmed);
    let cleaned = if let Some(rest) = folded_prefix
        .strip_prefix("to ")
        .or_else(|| folded_prefix.strip_prefix("aby "))
        .or_else(|| folded_prefix.strip_prefix("zeby "))
    {
        let drop_chars = folded_prefix.chars().count() - rest.chars().count();
        let byte_idx = trimmed
            .char_indices()
            .nth(drop_chars)
            .map(|(i, _)| i)
            .unwrap_or(0);
        trimmed[byte_idx..].trim()
    } else {
        trimmed
    };
    if cleaned.is_empty() {
        raw.trim().to_string()
    } else {
        cleaned.to_string()
    }
}

fn make_concise_title(prompt: &str) -> String {
    let cleaned = prompt
        .lines()
        .next()
        .unwrap_or(prompt)
        .trim();
    let first_sentence = cleaned
        .split(['.', '!', '?'])
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(cleaned);
    let mut out = String::new();
    for (idx, ch) in first_sentence.chars().enumerate() {
        if idx == 0 {
            for uc in ch.to_uppercase() {
                out.push(uc);
            }
            continue;
        }
        if idx >= 56 {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    if out.is_empty() {
        "Scheduled task".to_string()
    } else {
        out
    }
}

pub fn parse_schedule_prompt_heuristic(
    raw_prompt: &str,
    now: DateTime<Utc>,
    timezone_offset_mins: i32,
    preferred_agent: Option<&str>,
) -> AiScheduleDraft {
    let raw = raw_prompt.trim();
    let mut tokens = tokenize_with_spans(raw);
    if tokens.len() > MAX_HEURISTIC_SCAN_TOKENS {
        tokens.truncate(MAX_HEURISTIC_SCAN_TOKENS);
    }
    let target_agent_id = detect_target_agent(&tokens, preferred_agent);
    let tz = fixed_offset_from_mins(timezone_offset_mins)
        .unwrap_or_else(|_| FixedOffset::east_opt(0).expect("UTC offset"));

    // 1. Direct 5-field cron expression embedded in prompt.
    if tokens.len() >= 5 {
        for i in 0..=tokens.len() - 5 {
            let candidate = format!(
                "{} {} {} {} {}",
                trim_edge_punct(tokens[i].raw),
                trim_edge_punct(tokens[i + 1].raw),
                trim_edge_punct(tokens[i + 2].raw),
                trim_edge_punct(tokens[i + 3].raw),
                trim_edge_punct(tokens[i + 4].raw)
            );
            let looks_like_cron = candidate.contains('*')
                || candidate.contains('/')
                || candidate.contains(',')
                || candidate.contains('-');
            if looks_like_cron && ParsedCron::parse(&candidate).is_ok() {
                let start = if i > 0 && tokens[i - 1].clean_folded == "cron" {
                    tokens[i - 1].start
                } else {
                    tokens[i].start
                };
                let prompt = remove_span_and_clean(raw, start, tokens[i + 4].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Cron,
                    run_at: None,
                    interval_secs: None,
                    daily_time: None,
                    cron_expr: Some(candidate.clone()),
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: format!("Scheduled via cron expression `{candidate}`."),
                    source: "heuristic".to_string(),
                };
            }
        }
    }

    // 2. Weekdays cron ("w dni robocze o HH:MM", "every weekday at HH:MM", "weekdays at HH:MM").
    for i in 0..tokens.len() {
        let mut matched_end_idx = None;
        if i + 2 < tokens.len()
            && tokens[i].clean_folded == "w"
            && tokens[i + 1].clean_folded == "dni"
            && tokens[i + 2].clean_folded == "robocze"
        {
            matched_end_idx = Some(i + 2);
        } else if i + 1 < tokens.len()
            && tokens[i].clean_folded == "dni"
            && tokens[i + 1].clean_folded == "robocze"
        {
            matched_end_idx = Some(i + 1);
        } else if i + 1 < tokens.len()
            && tokens[i].clean_folded == "every"
            && matches!(tokens[i + 1].clean_folded.as_str(), "weekday" | "weekdays")
        {
            matched_end_idx = Some(i + 1);
        } else if i + 2 < tokens.len()
            && tokens[i].clean_folded == "on"
            && tokens[i + 1].clean_folded == "weekdays"
        {
            matched_end_idx = Some(i + 1);
        } else if matches!(tokens[i].clean_folded.as_str(), "weekday" | "weekdays") {
            matched_end_idx = Some(i);
        } else if i + 4 < tokens.len()
            && tokens[i].clean_folded == "od"
            && tokens[i + 1].clean_folded == "poniedzialku"
            && tokens[i + 2].clean_folded == "do"
            && tokens[i + 3].clean_folded == "piatku"
        {
            matched_end_idx = Some(i + 3);
        }

        if let Some(end_idx) = matched_end_idx {
            let mut span_end = tokens[end_idx].end;
            let (mut hour, mut min) = (9u32, 0u32);
            let mut j = end_idx + 1;
            if j < tokens.len() && matches!(tokens[j].clean_folded.as_str(), "o" | "at") {
                j += 1;
            }
            if j < tokens.len() {
                if let Some((h, m, _)) = parse_hhmm_token(tokens[j].raw) {
                    hour = h;
                    min = m;
                    span_end = tokens[j].end;
                }
            }
            let cron_expr = format!("{min} {hour} * * 1-5");
            let prompt = remove_span_and_clean(raw, tokens[i].start, span_end);
            return AiScheduleDraft {
                title: make_concise_title(&prompt),
                prompt,
                kind: ScheduleKind::Cron,
                run_at: None,
                interval_secs: None,
                daily_time: None,
                cron_expr: Some(cron_expr.clone()),
                target_agent_id,
                timezone_offset_mins,
                explanation: format!(
                    "Runs Monday-Friday at {hour:02}:{min:02} (cron `{cron_expr}`)."
                ),
                source: "heuristic".to_string(),
            };
        }
    }

    // 3. Daily ("codziennie o HH:MM", "every day at HH:MM", "daily at HH:MM", "o HH:MM codziennie").
    for i in 0..tokens.len() {
        let is_daily_pair = i + 1 < tokens.len()
            && ((tokens[i].clean_folded == "every" && tokens[i + 1].clean_folded == "day")
                || (tokens[i].clean_folded == "kazdego"
                    && tokens[i + 1].clean_folded == "dnia"));
        let is_daily_single = matches!(
            tokens[i].clean_folded.as_str(),
            "codziennie" | "daily" | "everyday"
        );

        if is_daily_pair || is_daily_single {
            let mut span_start = tokens[i].start;
            let mut span_end = if is_daily_pair {
                tokens[i + 1].end
            } else {
                tokens[i].end
            };
            let mut daily_time_str: Option<String> = None;

            let mut j = if is_daily_pair { i + 2 } else { i + 1 };
            if j < tokens.len() && matches!(tokens[j].clean_folded.as_str(), "o" | "at") {
                j += 1;
            }
            if j < tokens.len() {
                if let Some((_, _, hhmm)) = parse_hhmm_token(tokens[j].raw) {
                    daily_time_str = Some(hhmm);
                    span_end = tokens[j].end;
                }
            }

            if daily_time_str.is_none() && i > 0 {
                if let Some((_, _, hhmm)) = parse_hhmm_token(tokens[i - 1].raw) {
                    daily_time_str = Some(hhmm);
                    span_start = if i >= 2
                        && matches!(tokens[i - 2].clean_folded.as_str(), "o" | "at")
                    {
                        tokens[i - 2].start
                    } else {
                        tokens[i - 1].start
                    };
                }
            }

            let daily_time = daily_time_str.unwrap_or_else(|| "09:00".to_string());
            let prompt = remove_span_and_clean(raw, span_start, span_end);
            return AiScheduleDraft {
                title: make_concise_title(&prompt),
                prompt,
                kind: ScheduleKind::Daily,
                run_at: None,
                interval_secs: None,
                daily_time: Some(daily_time.clone()),
                cron_expr: None,
                target_agent_id,
                timezone_offset_mins,
                explanation: format!("Runs every day at {daily_time}."),
                source: "heuristic".to_string(),
            };
        }
    }

    // 4. Interval ("co N minut", "every 15 minutes", "co godzine", "hourly", etc.).
    for i in 0..tokens.len() {
        let w = tokens[i].clean_folded.as_str();
        if matches!(w, "hourly") {
            let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i].end);
            return AiScheduleDraft {
                title: make_concise_title(&prompt),
                prompt,
                kind: ScheduleKind::Interval,
                run_at: None,
                interval_secs: Some(3600),
                daily_time: None,
                cron_expr: None,
                target_agent_id,
                timezone_offset_mins,
                explanation: "Runs every 3600 seconds (hourly).".to_string(),
                source: "heuristic".to_string(),
            };
        }

        if matches!(w, "co" | "every") && i + 1 < tokens.len() {
            let next_w = tokens[i + 1].clean_folded.as_str();
            if matches!(next_w, "godzine" | "hour") {
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Interval,
                    run_at: None,
                    interval_secs: Some(3600),
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: "Runs every 3600 seconds (1 hour).".to_string(),
                    source: "heuristic".to_string(),
                };
            }
            if matches!(next_w, "minute" | "minuty") {
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Interval,
                    run_at: None,
                    interval_secs: Some(60),
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: "Runs every 60 seconds (1 minute).".to_string(),
                    source: "heuristic".to_string(),
                };
            }
            if next_w == "kwadrans" {
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Interval,
                    run_at: None,
                    interval_secs: Some(900),
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: "Runs every 900 seconds (15 minutes).".to_string(),
                    source: "heuristic".to_string(),
                };
            }
            if i + 2 < tokens.len()
                && ((next_w == "pol" && tokens[i + 2].clean_folded == "godziny")
                    || (next_w == "half" && tokens[i + 2].clean_folded == "hour"))
            {
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 2].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Interval,
                    run_at: None,
                    interval_secs: Some(1800),
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: "Runs every 1800 seconds (30 minutes).".to_string(),
                    source: "heuristic".to_string(),
                };
            }

            if let Some((n, mult)) = parse_number_and_unit_single(next_w) {
                let secs = n
                    .saturating_mul(mult)
                    .clamp(MIN_INTERVAL_SECS, MAX_INTERVAL_SECS);
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Interval,
                    run_at: None,
                    interval_secs: Some(secs),
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: format!("Runs every {secs} seconds."),
                    source: "heuristic".to_string(),
                };
            }

            if i + 2 < tokens.len() {
                if let Ok(n) = next_w.parse::<u64>() {
                    if let Some(mult) = unit_multiplier(tokens[i + 2].clean_folded.as_str()) {
                        let secs = n
                            .saturating_mul(mult)
                            .clamp(MIN_INTERVAL_SECS, MAX_INTERVAL_SECS);
                        let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 2].end);
                        return AiScheduleDraft {
                            title: make_concise_title(&prompt),
                            prompt,
                            kind: ScheduleKind::Interval,
                            run_at: None,
                            interval_secs: Some(secs),
                            daily_time: None,
                            cron_expr: None,
                            target_agent_id,
                            timezone_offset_mins,
                            explanation: format!("Runs every {secs} seconds."),
                            source: "heuristic".to_string(),
                        };
                    }
                }
            }
        }
    }

    // 5. One-shot relative ("za 10 minut", "in 10 minutes", "za godzine", etc.).
    for i in 0..tokens.len() {
        let w = tokens[i].clean_folded.as_str();
        if matches!(w, "za" | "in") && i + 1 < tokens.len() {
            let next_w = tokens[i + 1].clean_folded.as_str();
            if matches!(next_w, "godzine") {
                let run_at = now
                    .checked_add_signed(Duration::seconds(3600))
                    .unwrap_or(now);
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Once,
                    run_at: Some(run_at),
                    interval_secs: None,
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: "Runs once in 1 hour.".to_string(),
                    source: "heuristic".to_string(),
                };
            }
            if matches!(next_w, "minute") {
                let run_at = now
                    .checked_add_signed(Duration::seconds(60))
                    .unwrap_or(now);
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Once,
                    run_at: Some(run_at),
                    interval_secs: None,
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: "Runs once in 1 minute.".to_string(),
                    source: "heuristic".to_string(),
                };
            }
            if let Some((n, mult)) = parse_number_and_unit_single(next_w) {
                let secs = n.saturating_mul(mult).clamp(1, MAX_INTERVAL_SECS);
                let run_at = Duration::try_seconds(secs as i64)
                    .and_then(|d| now.checked_add_signed(d))
                    .unwrap_or(now);
                let prompt = remove_span_and_clean(raw, tokens[i].start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Once,
                    run_at: Some(run_at),
                    interval_secs: None,
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: format!("Runs once in {secs} seconds."),
                    source: "heuristic".to_string(),
                };
            }
            if i + 2 < tokens.len() {
                if let Ok(n) = next_w.parse::<u64>() {
                    if let Some(mult) = unit_multiplier(tokens[i + 2].clean_folded.as_str()) {
                        let secs = n.saturating_mul(mult).clamp(1, MAX_INTERVAL_SECS);
                        let run_at = Duration::try_seconds(secs as i64)
                            .and_then(|d| now.checked_add_signed(d))
                            .unwrap_or(now);
                        let prompt =
                            remove_span_and_clean(raw, tokens[i].start, tokens[i + 2].end);
                        return AiScheduleDraft {
                            title: make_concise_title(&prompt),
                            prompt,
                            kind: ScheduleKind::Once,
                            run_at: Some(run_at),
                            interval_secs: None,
                            daily_time: None,
                            cron_expr: None,
                            target_agent_id,
                            timezone_offset_mins,
                            explanation: format!("Runs once in {secs} seconds."),
                            source: "heuristic".to_string(),
                        };
                    }
                }
            }
        }
    }

    // 6. Standalone "o HH:MM" / "at HH:MM" -> Once today (or tomorrow if already passed).
    for i in 0..tokens.len() {
        if matches!(tokens[i].clean_folded.as_str(), "o" | "at") && i + 1 < tokens.len() {
            if let Some((h, m, hhmm)) = parse_hhmm_token(tokens[i + 1].raw) {
                let mut span_start = tokens[i].start;
                let mut force_tomorrow = false;
                if i > 0 && matches!(tokens[i - 1].clean_folded.as_str(), "jutro" | "tomorrow") {
                    span_start = tokens[i - 1].start;
                    force_tomorrow = true;
                } else if i > 0 && matches!(tokens[i - 1].clean_folded.as_str(), "dzis" | "dzisiaj" | "today") {
                    span_start = tokens[i - 1].start;
                }
                let local_now = now.with_timezone(&tz);
                let time = NaiveTime::from_hms_opt(h, m, 0).expect("valid HH:MM");
                let target_date = if force_tomorrow {
                    local_now.date_naive().succ_opt().unwrap_or(local_now.date_naive())
                } else {
                    local_now.date_naive()
                };
                let mut candidate_utc = tz
                    .from_local_datetime(&NaiveDateTime::new(target_date, time))
                    .single()
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or(now);
                if !force_tomorrow && candidate_utc <= now {
                    let tomorrow = target_date.succ_opt().unwrap_or(target_date);
                    candidate_utc = tz
                        .from_local_datetime(&NaiveDateTime::new(tomorrow, time))
                        .single()
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or(now);
                }
                let prompt = remove_span_and_clean(raw, span_start, tokens[i + 1].end);
                return AiScheduleDraft {
                    title: make_concise_title(&prompt),
                    prompt,
                    kind: ScheduleKind::Once,
                    run_at: Some(candidate_utc),
                    interval_secs: None,
                    daily_time: None,
                    cron_expr: None,
                    target_agent_id,
                    timezone_offset_mins,
                    explanation: format!("Runs once at {hhmm}."),
                    source: "heuristic".to_string(),
                };
            }
        }
    }

    // 7. Fallback one-shot in 5 minutes.
    let clean_prompt = if raw.is_empty() {
        "Run scheduled task".to_string()
    } else {
        raw.to_string()
    };
    AiScheduleDraft {
        title: make_concise_title(&clean_prompt),
        prompt: clean_prompt,
        kind: ScheduleKind::Once,
        run_at: Some(now.checked_add_signed(Duration::minutes(5)).unwrap_or(now)),
        interval_secs: None,
        daily_time: None,
        cron_expr: None,
        target_agent_id,
        timezone_offset_mins,
        explanation: "Could not detect an explicit cadence; defaulted to once in 5 minutes.".to_string(),
        source: "heuristic".to_string(),
    }
}

#[derive(Debug, Deserialize)]
struct LlmScheduleJson {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    kind: Option<ScheduleKind>,
    #[serde(default)]
    run_at: Option<String>,
    #[serde(default)]
    interval_secs: Option<u64>,
    #[serde(default)]
    daily_time: Option<String>,
    #[serde(default)]
    cron_expr: Option<String>,
    #[serde(default)]
    target_agent_id: Option<String>,
    #[serde(default)]
    explanation: Option<String>,
}

fn strip_think_and_fences(raw: &str) -> String {
    let mut cleaned = raw.trim().to_string();
    for close_tag in ["</think>", "</seed:think>"] {
        let lower = cleaned.to_ascii_lowercase();
        let open_tag = if close_tag == "</think>" {
            "<think>"
        } else {
            "<seed:think>"
        };
        if let Some(end) = lower.find(close_tag) {
            if !lower[..end].contains(open_tag) {
                cleaned = cleaned[end + close_tag.len()..].trim().to_string();
            }
        }
    }
    loop {
        let lower = cleaned.to_ascii_lowercase();
        let start = lower.find("<think>").or_else(|| lower.find("<seed:think>"));
        let Some(open_idx) = start else {
            break;
        };
        let close_len = if lower[open_idx..].starts_with("<seed:think>") {
            "</seed:think>"
        } else {
            "</think>"
        };
        if let Some(rel_end) = lower[open_idx..].find(close_len) {
            let end_idx = open_idx + rel_end + close_len.len();
            cleaned = format!("{}{}", &cleaned[..open_idx], &cleaned[end_idx..]);
        } else {
            cleaned = cleaned[..open_idx].to_string();
            break;
        }
    }

    let trimmed = cleaned.trim();
    if let Some(rest) = trimmed.strip_prefix("```json") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim().to_string();
        }
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim().to_string();
        }
    }
    trimmed.to_string()
}

fn extract_first_json_object(input: &str) -> Option<&str> {
    let start = input.find('{')?;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (rel_idx, ch) in input[start..].char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let end = start + rel_idx + ch.len_utf8();
                    return Some(&input[start..end]);
                }
            }
            _ => {}
        }
    }
    None
}

pub async fn parse_schedule_with_ai(
    inference: Option<&Arc<dyn InferenceProvider>>,
    agents: &[AgentDefinition],
    raw_prompt: &str,
    now: DateTime<Utc>,
    timezone_offset_mins: i32,
    preferred_agent: Option<&str>,
    model_hint: Option<&str>,
) -> AiScheduleDraft {
    let heuristic =
        parse_schedule_prompt_heuristic(raw_prompt, now, timezone_offset_mins, preferred_agent);

    if raw_prompt.trim().is_empty() {
        return heuristic;
    }

    let resolved_model = model_hint
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            agents
                .iter()
                .find(|a| a.enabled && a.id == "commander")
                .and_then(|a| a.model_config.model_override.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        });

    let (Some(inf), Some(model_name)) = (inference, resolved_model) else {
        return heuristic;
    };

    let mut agent_list = String::new();
    for a in agents.iter().filter(|a| a.enabled) {
        agent_list.push_str(&format!("- `{}` ({}): {}\n", a.id, a.name, a.description));
    }

    let sys_prompt = format!(
        "Extract an autonomous schedule from the user's request and reply ONLY with one valid JSON object. Write title, task prompt, and explanation in the exact same language as the user's request.\n\
         Schema:\n\
         {{\"title\":\"short title\",\"prompt\":\"clean task prompt for the agent\",\"kind\":\"once|interval|daily|cron\",\
         \"run_at\":null,\"interval_secs\":null,\"daily_time\":null,\"cron_expr\":null,\"target_agent_id\":null,\"explanation\":\"short explanation\"}}\n\
         Rules:\n\
         - Current UTC time: {}\n\
         - User timezone offset minutes: {}\n\
         - Preferred agent: {}\n\
         - `interval_secs` must be >= 10 when kind is `interval`.\n\
         - `daily_time` must be `HH:MM` (user local timezone) when kind is `daily`.\n\
         - `cron_expr` must be 5 fields `min hour dom month dow` (user local timezone) when kind is `cron`.\n\
         - `run_at` must be RFC3339 UTC timestamp when kind is `once`.\n\
         Available agents:\n{}",
        now.to_rfc3339(),
        timezone_offset_mins,
        preferred_agent.unwrap_or("none"),
        agent_list
    );

    let req = InferenceRequest {
        model: Some(model_name),
        api_mode: ApiMode::ChatCompletions,
        messages: vec![
            InferenceMessage {
                role: "system".to_string(),
                content: sys_prompt,
                ..Default::default()
            },
            InferenceMessage {
                role: "user".to_string(),
                content: raw_prompt.trim().to_string(),
                ..Default::default()
            },
        ],
        tools: Vec::new(),
        temperature: 0.0,
        max_output_tokens: 256,
        context_limit: 4096,
        stream: false,
        thinking_level: ThinkingLevel::Off,
        slot_id: None,
    };

    let Ok(Ok(resp)) = tokio::time::timeout(
        std::time::Duration::from_secs(12),
        inf.generate(req, CancellationToken::new()),
    )
    .await
    else {
        return heuristic;
    };

    let cleaned = strip_think_and_fences(&resp.content);
    let json_slice = extract_first_json_object(&cleaned).unwrap_or(&cleaned);
    let Ok(parsed) = serde_json::from_str::<LlmScheduleJson>(json_slice) else {
        return heuristic;
    };

    let Some(kind) = parsed.kind else {
        return heuristic;
    };

    let parsed_run_at = parsed
        .run_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s.trim()).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            if kind == ScheduleKind::Once {
                heuristic
                    .run_at
                    .or_else(|| Some(now.checked_add_signed(Duration::minutes(5)).unwrap_or(now)))
            } else {
                None
            }
        });

    let candidate_prompt = parsed
        .prompt
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| heuristic.prompt.clone());

    let candidate_title = parsed
        .title
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| make_concise_title(&candidate_prompt));

    let candidate_agent = parsed
        .target_agent_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter(|id| agents.is_empty() || agents.iter().any(|a| a.id == *id))
        .map(str::to_string)
        .or(heuristic.target_agent_id.clone());

    let test_record = ScheduleRecord {
        id: "draft-validate".to_string(),
        title: candidate_title.clone(),
        prompt: candidate_prompt.clone(),
        target_agent_id: candidate_agent.clone(),
        conversation_id: None,
        kind,
        run_at: parsed_run_at,
        interval_secs: parsed.interval_secs,
        daily_time: parsed
            .daily_time
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        cron_expr: parsed
            .cron_expr
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        timezone_offset_mins,
        enabled: true,
        next_run_at: None,
        last_run_at: None,
        last_status: None,
        last_error: None,
        last_task_id: None,
        run_count: 0,
        created_at: now,
        updated_at: now,
    };

    let Ok(Some(_)) = compute_next_run_at(&test_record, now) else {
        return heuristic;
    };

    AiScheduleDraft {
        title: candidate_title,
        prompt: candidate_prompt,
        kind,
        run_at: test_record.run_at,
        interval_secs: test_record.interval_secs,
        daily_time: test_record.daily_time,
        cron_expr: test_record.cron_expr,
        target_agent_id: candidate_agent,
        timezone_offset_mins,
        explanation: parsed
            .explanation
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or(heuristic.explanation),
        source: "llm".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ps5_agent_core::{InferenceStreamChunk, ProviderHealth, TokenUsage};
    use tokio::sync::mpsc;

    fn dt_utc(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).unwrap()
    }

    fn base_schedule(kind: ScheduleKind, now: DateTime<Utc>) -> ScheduleRecord {
        ScheduleRecord {
            id: "sched-1".into(),
            title: "Test".into(),
            prompt: "Run check".into(),
            target_agent_id: None,
            conversation_id: None,
            kind,
            run_at: None,
            interval_secs: None,
            daily_time: None,
            cron_expr: None,
            timezone_offset_mins: 0,
            enabled: true,
            next_run_at: None,
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_task_id: None,
            run_count: 0,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn compute_next_run_once_and_disabled() {
        let now = dt_utc(2026, 11, 13, 10, 0, 0);
        let mut sched = base_schedule(ScheduleKind::Once, now);
        sched.run_at = Some(now + Duration::minutes(15));
        assert_eq!(
            compute_next_run_at(&sched, now).unwrap(),
            Some(now + Duration::minutes(15))
        );

        sched.run_count = 1;
        assert_eq!(compute_next_run_at(&sched, now).unwrap(), None);

        sched.run_count = 0;
        sched.enabled = false;
        assert_eq!(compute_next_run_at(&sched, now).unwrap(), None);
    }

    #[test]
    fn compute_next_run_interval_validates_minimum() {
        let now = dt_utc(2026, 11, 13, 10, 0, 0);
        let mut sched = base_schedule(ScheduleKind::Interval, now);
        sched.interval_secs = Some(9);
        assert!(compute_next_run_at(&sched, now).is_err());

        sched.interval_secs = Some(300);
        assert_eq!(
            compute_next_run_at(&sched, now).unwrap(),
            Some(dt_utc(2026, 11, 13, 10, 5, 0))
        );
    }

    #[test]
    fn compute_next_run_daily_respects_timezone_offset() {
        // 2026-11-13 07:30 UTC == 08:30 CET (+60 min)
        let now = dt_utc(2026, 11, 13, 7, 30, 0);
        let mut sched = base_schedule(ScheduleKind::Daily, now);
        sched.timezone_offset_mins = 60;
        sched.daily_time = Some("09:00".into());
        // 09:00 local (+01:00) today == 08:00 UTC
        assert_eq!(
            compute_next_run_at(&sched, now).unwrap(),
            Some(dt_utc(2026, 11, 13, 8, 0, 0))
        );

        // 08:00 local (+01:00) today is 07:00 UTC (< 07:30 UTC) -> tomorrow 07:00 UTC
        sched.daily_time = Some("08:00".into());
        assert_eq!(
            compute_next_run_at(&sched, now).unwrap(),
            Some(dt_utc(2026, 11, 14, 7, 0, 0))
        );

        sched.daily_time = Some("25:99".into());
        assert!(compute_next_run_at(&sched, now).is_err());
    }

    #[test]
    fn compute_next_run_cron_weekdays_sunday_and_invalid() {
        // 2026-11-13 is Friday
        let fri = dt_utc(2026, 11, 13, 9, 30, 0);
        let mut sched = base_schedule(ScheduleKind::Cron, fri);
        sched.cron_expr = Some("0 9 * * 1-5".into());
        // Friday 09:00 already passed -> Monday 2026-11-16 09:00 UTC
        assert_eq!(
            compute_next_run_at(&sched, fri).unwrap(),
            Some(dt_utc(2026, 11, 16, 9, 0, 0))
        );

        // Both dow 0 and dow 7 represent Sunday (2026-11-15)
        sched.cron_expr = Some("15 10 * * 7".into());
        assert_eq!(
            compute_next_run_at(&sched, fri).unwrap(),
            Some(dt_utc(2026, 11, 15, 10, 15, 0))
        );
        sched.cron_expr = Some("15 10 * * 0".into());
        assert_eq!(
            compute_next_run_at(&sched, fri).unwrap(),
            Some(dt_utc(2026, 11, 15, 10, 15, 0))
        );

        // Invalid syntax and impossible calendar date (Feb 31)
        sched.cron_expr = Some("bad cron".into());
        assert!(compute_next_run_at(&sched, fri).is_err());
        sched.cron_expr = Some("0 0 31 2 *".into());
        assert!(compute_next_run_at(&sched, fri).is_err());
    }

    #[test]
    fn heuristic_parses_polish_and_english_prompts() {
        let now = dt_utc(2026, 11, 13, 12, 0, 0);

        let d1 = parse_schedule_prompt_heuristic(
            "Codziennie o 08:00 przygotuj raport statusu",
            now,
            60,
            None,
        );
        assert_eq!(d1.kind, ScheduleKind::Daily);
        assert_eq!(d1.daily_time.as_deref(), Some("08:00"));
        assert_eq!(d1.source, "heuristic");
        assert!(d1.prompt.to_lowercase().contains("przygotuj raport statusu"));

        let d2 = parse_schedule_prompt_heuristic(
            "Co 30 minut sprawdzaj logi i pamięć",
            now,
            60,
            None,
        );
        assert_eq!(d2.kind, ScheduleKind::Interval);
        assert_eq!(d2.interval_secs, Some(1800));
        assert!(d2.prompt.contains("pamięć"));

        let d3 = parse_schedule_prompt_heuristic(
            "Za 10 minut przypomnij Commanderowi o teście",
            now,
            0,
            None,
        );
        assert_eq!(d3.kind, ScheduleKind::Once);
        assert_eq!(d3.run_at, Some(now + Duration::minutes(10)));
        assert_eq!(d3.target_agent_id.as_deref(), Some("commander"));

        let d4 = parse_schedule_prompt_heuristic(
            "W dni robocze o 09:00 sprawdź pliki",
            now,
            60,
            None,
        );
        assert_eq!(d4.kind, ScheduleKind::Cron);
        assert_eq!(d4.cron_expr.as_deref(), Some("0 9 * * 1-5"));

        let d5 = parse_schedule_prompt_heuristic(
            "Every weekday at 09:00 summarize workspace files",
            now,
            0,
            Some("analyst"),
        );
        assert_eq!(d5.kind, ScheduleKind::Cron);
        assert_eq!(d5.cron_expr.as_deref(), Some("0 9 * * 1-5"));
        assert_eq!(d5.target_agent_id.as_deref(), Some("analyst"));

        let d6 = parse_schedule_prompt_heuristic(
            "Every 15 minutes check PS5LM health",
            now,
            0,
            None,
        );
        assert_eq!(d6.kind, ScheduleKind::Interval);
        assert_eq!(d6.interval_secs, Some(900));
    }

    struct MockScheduleInference {
        reply: String,
    }

    #[async_trait]
    impl InferenceProvider for MockScheduleInference {
        async fn generate(
            &self,
            _req: InferenceRequest,
            _cancel: CancellationToken,
        ) -> Result<ps5_agent_core::InferenceResponse> {
            Ok(ps5_agent_core::InferenceResponse {
                content: self.reply.clone(),
                reasoning: None,
                tool_calls: Vec::new(),
                model: "mock-qwen".into(),
                finish_reason: Some("stop".into()),
                usage: TokenUsage::default(),
                latency_ms: 5,
                timings: None,
            })
        }

        async fn generate_stream(
            &self,
            _req: InferenceRequest,
            _tx: mpsc::Sender<InferenceStreamChunk>,
            _cancel: CancellationToken,
        ) -> Result<ps5_agent_core::InferenceResponse> {
            unreachable!()
        }

        async fn health_check(&self) -> ProviderHealth {
            ProviderHealth {
                reachable: true,
                loaded_model: Some("mock-qwen".into()),
                available_models: vec!["mock-qwen".into()],
                error: None,
            }
        }

        async fn list_models(&self) -> Result<Vec<String>> {
            Ok(vec!["mock-qwen".into()])
        }
    }

    #[tokio::test]
    async fn parse_schedule_with_ai_uses_llm_and_falls_back_cleanly() {
        let now = dt_utc(2026, 11, 13, 12, 0, 0);
        let good_inf: Arc<dyn InferenceProvider> = Arc::new(MockScheduleInference {
            reply: "<think>analyzing</think>```json\n{\"title\":\"Disk Audit\",\"prompt\":\"Check disk usage\",\"kind\":\"interval\",\"interval_secs\":600,\"target_agent_id\":null,\"explanation\":\"Runs every 10 minutes.\"}\n```".into(),
        });
        let draft = parse_schedule_with_ai(
            Some(&good_inf),
            &[],
            "Every 10 minutes check disk usage",
            now,
            0,
            None,
            Some("mock-qwen"),
        )
        .await;
        assert_eq!(draft.source, "llm");
        assert_eq!(draft.kind, ScheduleKind::Interval);
        assert_eq!(draft.interval_secs, Some(600));

        let bad_inf: Arc<dyn InferenceProvider> = Arc::new(MockScheduleInference {
            reply: "not valid json".into(),
        });
        let fallback = parse_schedule_with_ai(
            Some(&bad_inf),
            &[],
            "Co 30 minut sprawdzaj logi",
            now,
            60,
            None,
            Some("mock-qwen"),
        )
        .await;
        assert_eq!(fallback.source, "heuristic");
        assert_eq!(fallback.kind, ScheduleKind::Interval);
        assert_eq!(fallback.interval_secs, Some(1800));
    }

    #[test]
    fn overflow_and_negative_countdown_edges_do_not_panic() {
        let now = dt_utc(2026, 11, 13, 12, 0, 0);

        // Overdue target must yield 0s remaining, never a negative/wrapped u64.
        assert_eq!(seconds_until_next_run(Some(now - Duration::seconds(42)), now), Some(0));
        assert_eq!(seconds_until_next_run(Some(now), now), Some(0));
        assert_eq!(seconds_until_next_run(Some(now + Duration::seconds(90)), now), Some(90));
        assert_eq!(seconds_until_next_run(None, now), None);

        // Extreme future timestamp overflow returns Err instead of panicking.
        let max_utc = DateTime::<Utc>::MAX_UTC;
        let mut sched = base_schedule(ScheduleKind::Interval, now);
        sched.interval_secs = Some(60);
        assert!(compute_next_run_at(&sched, max_utc).is_err());

        let mut cron_sched = base_schedule(ScheduleKind::Cron, now);
        cron_sched.cron_expr = Some("0 9 * * *".into());
        assert!(compute_next_run_at(&cron_sched, max_utc).is_err());

        // Huge unit / negative numeric prompt values saturate or fall back safely.
        let huge = parse_schedule_prompt_heuristic(
            "Co 9999999999999999999 godzin sprawdzaj dysk",
            now,
            0,
            None,
        );
        assert_eq!(huge.kind, ScheduleKind::Interval);
        assert_eq!(huge.interval_secs, Some(MAX_INTERVAL_SECS));

        let mut huge_rec = base_schedule(ScheduleKind::Interval, now);
        huge_rec.interval_secs = Some(u64::MAX);
        assert!(compute_next_run_at(&huge_rec, now).is_err());

        let neg = parse_schedule_prompt_heuristic("Za -10 minut ping", now, 0, None);
        assert_eq!(neg.kind, ScheduleKind::Once);
        assert!(neg.run_at.unwrap() >= now);
    }

    #[test]
    fn detect_user_language_matches_user_turn_across_polish_english_and_ambiguous_inputs() {
        assert_eq!(detect_user_language_name("dzien dobry"), "Polish");
        assert_eq!(
            detect_user_language_name("executor slyszusz mnie"),
            "Polish"
        );
        assert_eq!(detect_user_language_name("hallo"), "English");
        assert_eq!(detect_user_language_name("hallo commander"), "English");
        assert_eq!(detect_user_language_name("hey hear me"), "English");
        assert_eq!(
            detect_user_language_name("powiedz executorowi ze ma odpisac czesc"),
            "Polish"
        );
        assert_eq!(detect_user_language_name("ok"), "auto");
        assert_eq!(detect_user_language_name("123"), "auto");
        assert_eq!(detect_user_language_name(""), "auto");

        // Additional accented / multilingual sanity checks
        assert_eq!(
            detect_user_language_name("Proszę sprawdzić logi i pamięć"),
            "Polish"
        );
        assert_eq!(
            detect_user_language_name("hallo, wie geht es dir?"),
            "German"
        );
        assert_eq!(
            detect_user_language_name("¿Cómo estás? gracias"),
            "Spanish"
        );
        assert_eq!(detect_user_language_name("bonjour, merci beaucoup"), "French");
        assert_eq!(
            detect_user_language_name("Can you check what happened with jest?"),
            "English"
        );

        assert!(language_response_directive("hey hear me").contains("Respond strictly in English."));
        assert!(
            language_response_directive("dzien dobry").contains("Respond strictly in Polish.")
        );
        assert_eq!(
            language_response_directive("ok"),
            "Reply in the exact same language as the user's latest message above."
        );
    }

    struct CapturingScheduleInference {
        captured_system: Arc<std::sync::Mutex<Option<String>>>,
        reply: String,
    }

    #[async_trait]
    impl InferenceProvider for CapturingScheduleInference {
        async fn generate(
            &self,
            req: InferenceRequest,
            _cancel: CancellationToken,
        ) -> Result<ps5_agent_core::InferenceResponse> {
            let sys = req
                .messages
                .iter()
                .find(|m| m.role == "system")
                .map(|m| m.content.clone());
            *self.captured_system.lock().unwrap() = sys;
            Ok(ps5_agent_core::InferenceResponse {
                content: self.reply.clone(),
                reasoning: None,
                tool_calls: Vec::new(),
                model: "mock-qwen".into(),
                finish_reason: Some("stop".into()),
                usage: TokenUsage::default(),
                latency_ms: 5,
                timings: None,
            })
        }

        async fn generate_stream(
            &self,
            _req: InferenceRequest,
            _tx: mpsc::Sender<InferenceStreamChunk>,
            _cancel: CancellationToken,
        ) -> Result<ps5_agent_core::InferenceResponse> {
            unreachable!()
        }

        async fn health_check(&self) -> ProviderHealth {
            ProviderHealth {
                reachable: true,
                loaded_model: Some("mock-qwen".into()),
                available_models: vec!["mock-qwen".into()],
                error: None,
            }
        }

        async fn list_models(&self) -> Result<Vec<String>> {
            Ok(vec!["mock-qwen".into()])
        }
    }

    #[tokio::test]
    async fn parse_schedule_with_ai_prompt_has_no_polish_bias_and_keeps_request_language() {
        let now = dt_utc(2026, 11, 13, 12, 0, 0);
        let captured = Arc::new(std::sync::Mutex::new(None));
        let inf: Arc<dyn InferenceProvider> = Arc::new(CapturingScheduleInference {
            captured_system: captured.clone(),
            reply: "{\"title\":\"Health Ping\",\"prompt\":\"Check health\",\"kind\":\"interval\",\"interval_secs\":300,\"target_agent_id\":null,\"explanation\":\"Every 5 minutes.\"}".into(),
        });
        let draft = parse_schedule_with_ai(
            Some(&inf),
            &[],
            "Every 5 minutes check health",
            now,
            0,
            None,
            Some("mock-qwen"),
        )
        .await;
        assert_eq!(draft.source, "llm");
        let sys_prompt = captured.lock().unwrap().clone().expect("system prompt");
        assert!(!sys_prompt.contains("Polish or English"));
        assert!(sys_prompt.contains("exact same language as the user's request"));
    }
}
