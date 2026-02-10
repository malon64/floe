use crate::io::format::{self, AcceptedSinkAdapter, InputAdapter, RejectedSinkAdapter};
use crate::io::storage::CloudClient;
use crate::FloeResult;

pub trait Runtime {
    fn input_adapter(&self, format: &str) -> FloeResult<&'static dyn InputAdapter>;
    fn accepted_sink_adapter(&self, format: &str) -> FloeResult<&'static dyn AcceptedSinkAdapter>;
    fn rejected_sink_adapter(&self, format: &str) -> FloeResult<&'static dyn RejectedSinkAdapter>;
    fn storage(&mut self) -> &mut CloudClient;
}

pub struct DefaultRuntime {
    cloud: CloudClient,
}

impl DefaultRuntime {
    pub fn new() -> Self {
        Self {
            cloud: CloudClient::new(),
        }
    }
}

impl Default for DefaultRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime for DefaultRuntime {
    fn input_adapter(&self, format: &str) -> FloeResult<&'static dyn InputAdapter> {
        format::input_adapter(format)
    }

    fn accepted_sink_adapter(&self, format: &str) -> FloeResult<&'static dyn AcceptedSinkAdapter> {
        format::accepted_sink_adapter(format)
    }

    fn rejected_sink_adapter(&self, format: &str) -> FloeResult<&'static dyn RejectedSinkAdapter> {
        format::rejected_sink_adapter(format)
    }

    fn storage(&mut self) -> &mut CloudClient {
        &mut self.cloud
    }
}
