#[derive(Debug, Clone)]
pub enum RunEvent {
    RunStarted { run_id: String },
    EntityStarted { name: String },
    EntityFinished { name: String },
    RunFinished { run_id: String },
}

pub trait RunObserver: Send + Sync {
    fn on_event(&self, event: RunEvent);
}

pub struct NoopObserver;

impl RunObserver for NoopObserver {
    fn on_event(&self, _event: RunEvent) {}
}

static NOOP_OBSERVER: NoopObserver = NoopObserver;

pub fn default_observer() -> &'static dyn RunObserver {
    &NOOP_OBSERVER
}
