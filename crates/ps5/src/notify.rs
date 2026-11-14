//! System toasts via `sceNotificationSend`. Feature `notify`.

use crate::{buf::Buf, check, Result};
use ps5_sys::{sceNotificationSend, SCE_NOTIFICATION_LOCAL_USER_ID_SYSTEM};

/// A toast with a title line and an optional second line.
///
/// `InteractiveToastTemplateB` puts `message` on the top line, so a lone
/// message sits at the top; `sub` fills the line under it.
#[derive(Clone, Copy)]
pub struct Toast<'a> {
    pub message: &'a str,
    pub sub: Option<&'a str>,
}

fn truncate_str_for_json(s: &str, max_escaped_bytes: usize) -> (&str, usize) {
    let mut used = 0usize;
    let mut end = 0usize;
    for (idx, ch) in s.char_indices() {
        let need = match ch {
            '"' | '\\' | '\n' => 2,
            ch if (ch as u32) <= 0x1F => 0,
            _ => ch.len_utf8(),
        };
        if used + need > max_escaped_bytes {
            return (&s[..idx], used);
        }
        used += need;
        end = idx + ch.len_utf8();
    }
    (&s[..end], used)
}

impl<'a> Toast<'a> {
    pub const fn new(message: &'a str) -> Self {
        Self { message, sub: None }
    }

    pub const fn sub(mut self, sub: &'a str) -> Self {
        self.sub = Some(sub);
        self
    }

    pub fn send(&self) -> Result<()> {
        const PREFIX: &[u8] = br#"{"rawData":{"viewTemplateType":"InteractiveToastTemplateB","channelType":"Downloads","useCaseId":"IDC","toastOverwriteType":"No","isImmediate":true,"priority":100,"viewData":{"icon":{"type":"Predefined","parameters":{"icon":"download"}},"message":{"body":""#;
        const MID_CLOSE_MSG: &[u8] = br#""}"#;
        const SUB_OPEN: &[u8] = br#","subMessage":{"body":""#;
        const SUB_CLOSE: &[u8] = br#""}"#;
        const SUFFIX: &[u8] = br#"}},"createdDateTime":"2025-12-14T03:14:51.473Z","localNotificationId":"588193127"}"#;
        // Buf::<1024> holds 1023 payload bytes plus the trailing NUL terminator.
        const MAX_PAYLOAD: usize = 1023;

        let fixed_overhead = PREFIX.len()
            + MID_CLOSE_MSG.len()
            + SUFFIX.len()
            + if self.sub.is_some() {
                SUB_OPEN.len() + SUB_CLOSE.len()
            } else {
                0
            };
        let text_budget = MAX_PAYLOAD.saturating_sub(fixed_overhead);
        let msg_cap = if let Some(sub) = self.sub {
            let (_, sub_reserve) = truncate_str_for_json(sub, text_budget / 2);
            text_budget.saturating_sub(sub_reserve)
        } else {
            text_budget
        };
        let (msg_slice, msg_used) = truncate_str_for_json(self.message, msg_cap);

        let mut json = Buf::<1024>::new();
        json.push(PREFIX);
        json.push_json_escaped(msg_slice);
        json.push(MID_CLOSE_MSG);
        if let Some(sub) = self.sub {
            let (sub_slice, _) =
                truncate_str_for_json(sub, text_budget.saturating_sub(msg_used));
            json.push(SUB_OPEN);
            json.push_json_escaped(sub_slice);
            json.push(SUB_CLOSE);
        }
        json.push(SUFFIX);

        check(unsafe {
            sceNotificationSend(SCE_NOTIFICATION_LOCAL_USER_ID_SYSTEM as i32, true, json.as_ptr())
        })
    }
}

/// Classic plain-text popup via the kernel (no JSON, no extra lib).
pub fn simple(message: &str) -> Result<()> {
    let mut req: ps5_sys::notify_request_t = unsafe { core::mem::zeroed() };
    let n = message.len().min(req.message.len() - 1);
    for (d, &s) in req.message.iter_mut().zip(&message.as_bytes()[..n]) {
        *d = s as _;
    }
    check(unsafe {
        ps5_sys::sceKernelSendNotificationRequest(0, &mut req, core::mem::size_of_val(&req), 0)
    })
}

/// Shorthand for a single-line toast.
pub fn notify(message: &str) -> Result<()> {
    Toast::new(message).send()
}
