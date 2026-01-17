import gleam/http
import gleam/http/request
import gleam/httpc
import gleam/int
import gleam/io
import gleam/json
import gleam/result
import gleam/string
import reddit_cli_args

pub fn main() -> Nil {
  let args = reddit_cli_args.get()
  case args {
    [base_url, command, ..rest] -> run_command(base_url, command, rest)
    _ -> print_usage()
  }
}

fn run_command(base_url: String, command: String, args: List(String)) -> Nil {
  let normalized_url = normalize_base_url(base_url)
  case string.lowercase(command) {
    "register" -> run_register(normalized_url, args)
    "create-subreddit" -> run_subreddit(normalized_url, args, True)
    "join-subreddit" -> run_membership(normalized_url, args, True)
    "leave-subreddit" -> run_membership(normalized_url, args, False)
    "post" -> run_post(normalized_url, args)
    "comment" -> run_comment(normalized_url, args)
    "vote-post" -> run_vote(normalized_url, args, True)
    "vote-comment" -> run_vote(normalized_url, args, False)
    "feed" -> run_feed(normalized_url, args)
    "send-message" -> run_message(normalized_url, args)
    "messages" -> run_get_messages(normalized_url, args)
    _ -> {
      io.println_error("Unknown command\n")
      print_usage()
    }
  }
}

fn run_register(base_url: String, args: List(String)) -> Nil {
  case args {
    [username] -> {
      let body = json.object([#("username", json.string(username))])
      post_and_print(base_url, "/api/accounts", body)
    }
    _ -> io.println_error("register requires <username>")
  }
}

fn run_subreddit(base_url: String, args: List(String), creating: Bool) -> Nil {
  case args {
    [username, name] -> {
      let path = "/api/subreddits"
      let body =
        json.object([
          #("username", json.string(username)),
          #("name", json.string(name)),
        ])
      case creating {
        True -> post_and_print(base_url, path, body)
        False -> io.println_error("Unknown subreddit command")
      }
    }
    _ -> io.println_error("create-subreddit requires <username> <name>")
  }
}

fn run_membership(base_url: String, args: List(String), joining: Bool) -> Nil {
  case args {
    [username, name] -> {
      let path = case joining {
        True -> "/api/subreddits/join"
        False -> "/api/subreddits/leave"
      }
      let body =
        json.object([
          #("username", json.string(username)),
          #("name", json.string(name)),
        ])
      post_and_print(base_url, path, body)
    }
    _ -> {
      let action = case joining {
        True -> "join-subreddit"
        False -> "leave-subreddit"
      }
      io.println_error(string.append(action, " requires <username> <name>"))
    }
  }
}

fn run_post(base_url: String, args: List(String)) -> Nil {
  case args {
    [username, subreddit, content] -> {
      let body =
        json.object([
          #("username", json.string(username)),
          #("subreddit", json.string(subreddit)),
          #("content", json.string(content)),
          #("is_repost", json.bool(False)),
          #("original_post_id", json.null()),
        ])
      post_and_print(base_url, "/api/posts", body)
    }
    [username, subreddit, content, original_id] -> {
      let body =
        json.object([
          #("username", json.string(username)),
          #("subreddit", json.string(subreddit)),
          #("content", json.string(content)),
          #("is_repost", json.bool(True)),
          #("original_post_id", json.string(original_id)),
        ])
      post_and_print(base_url, "/api/posts", body)
    }
    _ ->
      io.println_error(
        "post requires <username> <subreddit> <content> [original_post_id]",
      )
  }
}

fn run_comment(base_url: String, args: List(String)) -> Nil {
  case args {
    [username, post_id, content] -> {
      let body =
        json.object([
          #("username", json.string(username)),
          #("post_id", json.string(post_id)),
          #("content", json.string(content)),
          #("parent_comment_id", json.null()),
        ])
      post_and_print(base_url, "/api/comments", body)
    }
    [username, post_id, content, parent_id] -> {
      let body =
        json.object([
          #("username", json.string(username)),
          #("post_id", json.string(post_id)),
          #("content", json.string(content)),
          #("parent_comment_id", json.string(parent_id)),
        ])
      post_and_print(base_url, "/api/comments", body)
    }
    _ ->
      io.println_error(
        "comment requires <username> <post_id> <content> [parent_comment_id]",
      )
  }
}

fn run_vote(base_url: String, args: List(String), post_vote: Bool) -> Nil {
  let expected = case post_vote {
    True -> "<username> <post_id> <up|down>"
    False -> "<username> <comment_id> <up|down>"
  }

  case args {
    [username, target_id, direction] -> {
      let body = case post_vote {
        True ->
          json.object([
            #("username", json.string(username)),
            #("direction", json.string(direction)),
            #("post_id", json.string(target_id)),
            #("comment_id", json.null()),
          ])
        False ->
          json.object([
            #("username", json.string(username)),
            #("direction", json.string(direction)),
            #("post_id", json.null()),
            #("comment_id", json.string(target_id)),
          ])
      }
      post_and_print(base_url, "/api/votes", body)
    }
    _ ->
      io.println_error(string.append(
        case post_vote {
          True -> "vote-post requires "
          False -> "vote-comment requires "
        },
        expected,
      ))
  }
}

fn run_feed(base_url: String, args: List(String)) -> Nil {
  case args {
    [username] -> get_and_print(base_url, string.append("/api/feed/", username))
    _ -> io.println_error("feed requires <username>")
  }
}

fn run_message(base_url: String, args: List(String)) -> Nil {
  case args {
    [from, to, content] -> {
      let body =
        json.object([
          #("from", json.string(from)),
          #("to", json.string(to)),
          #("content", json.string(content)),
          #("parent_message_id", json.null()),
        ])
      post_and_print(base_url, "/api/messages", body)
    }
    [from, to, content, parent_id] -> {
      let body =
        json.object([
          #("from", json.string(from)),
          #("to", json.string(to)),
          #("content", json.string(content)),
          #("parent_message_id", json.string(parent_id)),
        ])
      post_and_print(base_url, "/api/messages", body)
    }
    _ ->
      io.println_error(
        "send-message requires <from> <to> <content> [parent_message_id]",
      )
  }
}

fn run_get_messages(base_url: String, args: List(String)) -> Nil {
  case args {
    [username] ->
      get_and_print(base_url, string.append("/api/messages/", username))
    _ -> io.println_error("messages requires <username>")
  }
}

fn post_and_print(base_url: String, path: String, body: json.Json) -> Nil {
  let url = build_url(base_url, path)
  let req_result =
    request.to(url)
    |> result.map_error(fn(_) { "Invalid base URL" })
    |> result.map(fn(req) {
      req
      |> request.set_method(http.Post)
      |> request.set_header("content-type", "application/json")
      |> request.set_body(json.to_string(body))
    })

  case req_result {
    Ok(req) -> print_response(send_request(req))
    Error(message) -> io.println_error(message)
  }
}

fn get_and_print(base_url: String, path: String) -> Nil {
  let url = build_url(base_url, path)
  case request.to(url) {
    Ok(req) -> print_response(send_request(req))
    Error(_) -> io.println_error("Invalid base URL")
  }
}

fn send_request(req: request.Request(String)) -> Result(#(Int, String), String) {
  case httpc.send(req) {
    Ok(resp) -> Ok(#(resp.status, resp.body))
    Error(err) -> Error(describe_http_error(err))
  }
}

fn print_response(result: Result(#(Int, String), String)) -> Nil {
  case result {
    Ok(#(status, body)) -> {
      io.println(string.append("HTTP ", int.to_string(status)))
      io.println(body)
    }
    Error(message) -> io.println_error(message)
  }
}

fn describe_http_error(error: httpc.HttpError) -> String {
  case error {
    httpc.InvalidUtf8Response -> "Server returned a non UTF-8 response"
    httpc.FailedToConnect(ip4, ip6) ->
      string.append(
        "Failed to connect over IPv4: ",
        string.append(
          describe_connect_error(ip4),
          string.append(", IPv6: ", describe_connect_error(ip6)),
        ),
      )
    httpc.ResponseTimeout -> "Request timed out"
  }
}

fn describe_connect_error(error: httpc.ConnectError) -> String {
  case error {
    httpc.Posix(code) -> string.append("POSIX ", code)
    httpc.TlsAlert(code, detail) ->
      string.append("TLS ", string.append(code, string.append(": ", detail)))
  }
}

fn build_url(base: String, path: String) -> String {
  let safe_base = normalize_base_url(base)
  case string.starts_with(path, "/") {
    True -> string.append(safe_base, path)
    False -> string.append(safe_base, string.append("/", path))
  }
}

fn normalize_base_url(base_url: String) -> String {
  let len = string.length(base_url)
  case string.ends_with(base_url, "/") {
    True ->
      case len > 1 {
        True -> normalize_base_url(string.slice(base_url, 0, len - 1))
        False -> base_url
      }
    False -> base_url
  }
}

fn print_usage() -> Nil {
  io.println("Usage: gleam run -m reddit_cli <base_url> <command> [args...]")
  io.println("Commands:")
  io.println("  register <username>")
  io.println("  create-subreddit <username> <name>")
  io.println("  join-subreddit <username> <name>")
  io.println("  leave-subreddit <username> <name>")
  io.println("  post <username> <subreddit> <content> [original_post_id]")
  io.println("  comment <username> <post_id> <content> [parent_comment_id]")
  io.println("  vote-post <username> <post_id> <up|down>")
  io.println("  vote-comment <username> <comment_id> <up|down>")
  io.println("  feed <username>")
  io.println("  send-message <from> <to> <content> [parent_message_id]")
  io.println("  messages <username>")
}
