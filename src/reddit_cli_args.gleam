@external(erlang, "reddit_cli_ffi", "argv")
fn raw_args() -> List(String)

pub fn get() -> List(String) {
  raw_args()
}
