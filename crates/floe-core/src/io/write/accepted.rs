pub use crate::io::format::AcceptedWriteRequest;

use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteOutput};
use crate::FloeResult;

pub fn write_with_adapter(
    adapter: &dyn AcceptedSinkAdapter,
    request: AcceptedWriteRequest<'_>,
) -> FloeResult<AcceptedWriteOutput> {
    adapter.write(request)
}
