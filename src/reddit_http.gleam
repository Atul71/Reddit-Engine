import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/http
import gleam/http/request
import gleam/http/response
import gleam/io
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import gleam/result
import gleam/string
import mist
import reddit

const body_limit = 1_048_576

const engine_timeout_ms = 5000

type SubredditPayload {
  SubredditPayload(username: String, name: String)
}

type PostPayload {
  PostPayload(
    username: String,
    subreddit: String,
    content: String,
    is_repost: Option(Bool),
    original_post_id: Option(String),
  )
}

type CommentPayload {
  CommentPayload(
    username: String,
    post_id: String,
    content: String,
    parent_comment_id: Option(String),
  )
}

type VotePayload {
  VotePayload(
    username: String,
    direction: String,
    post_id: Option(String),
    comment_id: Option(String),
  )
}

type MessagePayload {
  MessagePayload(
    from: String,
    to: String,
    content: String,
    parent_message_id: Option(String),
  )
}

pub fn main() -> Nil {
  start_server(4000)
}

pub fn start_server(port: Int) -> Nil {
  let engine = reddit.start_engine()
  let handler = fn(req) { route_request(req, engine) }

  let server =
    mist.new(handler)
    |> mist.port(port)
    |> mist.start

  case server {
    Ok(_) -> wait_forever()
    Error(error) -> io.println_error(start_error_message(error))
  }
}

fn wait_forever() -> Nil {
  process.sleep(60_000)
  wait_forever()
}

fn start_error_message(error: actor.StartError) -> String {
  case error {
    actor.InitTimeout -> "HTTP server failed to start: init timeout"
    actor.InitFailed(reason) ->
      string.append("HTTP server failed to start: ", reason)
    actor.InitExited(_reason) -> "HTTP server exited during init"
  }
}

fn route_request(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  let segments = request.path_segments(req)

  case req.method, segments {
    http.Get, ["api", "health"] ->
      ok_response(json.object([#("status", json.string("ok"))]))
    http.Post, ["api", "accounts"] -> handle_register(req, engine)
    http.Post, ["api", "subreddits"] -> handle_create_subreddit(req, engine)
    http.Post, ["api", "subreddits", "join"] ->
      handle_join_subreddit(req, engine)
    http.Post, ["api", "subreddits", "leave"] ->
      handle_leave_subreddit(req, engine)
    http.Post, ["api", "posts"] -> handle_create_post(req, engine)
    http.Post, ["api", "comments"] -> handle_create_comment(req, engine)
    http.Post, ["api", "votes"] -> handle_vote(req, engine)
    http.Get, ["api", "feed", username] -> handle_feed(username, engine)
    http.Post, ["api", "messages"] -> handle_send_message(req, engine)
    http.Get, ["api", "messages", username] ->
      handle_get_messages(username, engine)
    _, _ -> error_response(404, "Endpoint not found")
  }
}

fn handle_register(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  let decoder = {
    use username <- decode.field("username", decode.string)
    decode.success(username)
  }

  case decode_body(req, decoder) {
    Ok(username) -> {
      respond_with_status(engine, fn(reply) {
        reddit.RegisterAccount(username, reply)
      })
    }
    Error(message) -> error_response(400, message)
  }
}

fn handle_create_subreddit(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  let decoder = {
    use username <- decode.field("username", decode.string)
    use name <- decode.field("name", decode.string)
    decode.success(SubredditPayload(username, name))
  }

  case decode_body(req, decoder) {
    Ok(SubredditPayload(username, name)) -> {
      respond_with_status(engine, fn(reply) {
        reddit.CreateSubreddit(username, name, reply)
      })
    }
    Error(message) -> error_response(400, message)
  }
}

fn handle_join_subreddit(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  handle_subreddit_membership(req, engine, True)
}

fn handle_leave_subreddit(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  handle_subreddit_membership(req, engine, False)
}

fn handle_subreddit_membership(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
  joining: Bool,
) -> response.Response(mist.ResponseData) {
  let decoder = {
    use username <- decode.field("username", decode.string)
    use name <- decode.field("name", decode.string)
    decode.success(SubredditPayload(username, name))
  }

  case decode_body(req, decoder) {
    Ok(SubredditPayload(username, name)) -> {
      respond_with_status(engine, fn(reply) {
        case joining {
          True -> reddit.JoinSubreddit(username, name, reply)
          False -> reddit.LeaveSubreddit(username, name, reply)
        }
      })
    }
    Error(message) -> error_response(400, message)
  }
}

fn handle_create_post(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  let decoder = {
    use username <- decode.field("username", decode.string)
    use subreddit <- decode.field("subreddit", decode.string)
    use content <- decode.field("content", decode.string)
    use is_repost <- decode.field("is_repost", decode.optional(decode.bool))
    use original_post_id <- decode.field(
      "original_post_id",
      decode.optional(decode.string),
    )
    decode.success(PostPayload(
      username,
      subreddit,
      content,
      is_repost,
      original_post_id,
    ))
  }

  case decode_body(req, decoder) {
    Ok(PostPayload(username, subreddit, content, is_repost, original)) -> {
      let original_id = option.unwrap(original, "")
      let repost_flag = option.unwrap(is_repost, False)
      respond_with_status(engine, fn(reply) {
        reddit.CreatePost(
          username,
          subreddit,
          content,
          repost_flag,
          original_id,
          reply,
        )
      })
    }
    Error(message) -> error_response(400, message)
  }
}

fn handle_create_comment(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  let decoder = {
    use username <- decode.field("username", decode.string)
    use post_id <- decode.field("post_id", decode.string)
    use content <- decode.field("content", decode.string)
    use parent <- decode.field(
      "parent_comment_id",
      decode.optional(decode.string),
    )
    decode.success(CommentPayload(username, post_id, content, parent))
  }

  case decode_body(req, decoder) {
    Ok(CommentPayload(username, post_id, content, parent)) -> {
      respond_with_status(engine, fn(reply) {
        reddit.CreateComment(username, post_id, parent, content, reply)
      })
    }
    Error(message) -> error_response(400, message)
  }
}

fn handle_vote(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  let decoder = {
    use username <- decode.field("username", decode.string)
    use direction <- decode.field("direction", decode.string)
    use post_id <- decode.field("post_id", decode.optional(decode.string))
    use comment_id <- decode.field("comment_id", decode.optional(decode.string))
    decode.success(VotePayload(username, direction, post_id, comment_id))
  }

  case decode_body(req, decoder) {
    Ok(VotePayload(username, direction, post_id, comment_id)) -> {
      case string.lowercase(direction) {
        "up" -> {
          respond_with_status(engine, fn(reply) {
            reddit.Upvote(username, post_id, comment_id, reply)
          })
        }
        "down" -> {
          respond_with_status(engine, fn(reply) {
            reddit.Downvote(username, post_id, comment_id, reply)
          })
        }
        _ -> error_response(400, "direction must be \"up\" or \"down\"")
      }
    }
    Error(message) -> error_response(400, message)
  }
}

fn handle_feed(
  username: String,
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  case call_engine(engine, fn(reply) { reddit.GetFeed(username, reply) }) {
    Ok(reddit.FeedResult(posts)) -> {
      let json_posts = list.map(posts, post_to_json)
      ok_response(
        json.object([#("posts", json.preprocessed_array(json_posts))]),
      )
    }
    Ok(reddit.ErrorResponse(message)) -> error_response(400, message)
    Ok(_) -> error_response(500, "Unexpected engine response")
    Error(message) -> error_response(504, message)
  }
}

fn handle_send_message(
  req: request.Request(mist.Connection),
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  let decoder = {
    use from <- decode.field("from", decode.string)
    use to <- decode.field("to", decode.string)
    use content <- decode.field("content", decode.string)
    use parent <- decode.field(
      "parent_message_id",
      decode.optional(decode.string),
    )
    decode.success(MessagePayload(from, to, content, parent))
  }

  case decode_body(req, decoder) {
    Ok(MessagePayload(from, to, content, parent)) -> {
      respond_with_status(engine, fn(reply) {
        reddit.SendMessage(from, to, content, parent, reply)
      })
    }
    Error(message) -> error_response(400, message)
  }
}

fn handle_get_messages(
  username: String,
  engine: Subject(reddit.EngineMessage),
) -> response.Response(mist.ResponseData) {
  case call_engine(engine, fn(reply) { reddit.GetMessages(username, reply) }) {
    Ok(reddit.MessagesResult(messages)) -> {
      let json_messages = list.map(messages, message_to_json)
      ok_response(
        json.object([
          #("messages", json.preprocessed_array(json_messages)),
        ]),
      )
    }
    Ok(reddit.ErrorResponse(message)) -> error_response(400, message)
    Ok(_) -> error_response(500, "Unexpected engine response")
    Error(message) -> error_response(504, message)
  }
}

fn respond_with_status(
  engine: Subject(reddit.EngineMessage),
  build: fn(Subject(reddit.EngineResponse)) -> reddit.EngineMessage,
) -> response.Response(mist.ResponseData) {
  case call_engine(engine, build) {
    Ok(reddit.Success(message)) ->
      ok_response(
        json.object([
          #("status", json.string("ok")),
          #("message", json.string(message)),
        ]),
      )
    Ok(reddit.ErrorResponse(message)) -> error_response(400, message)
    Ok(_) -> error_response(500, "Unexpected engine response")
    Error(message) -> error_response(504, message)
  }
}

fn call_engine(
  engine: Subject(reddit.EngineMessage),
  build: fn(Subject(reddit.EngineResponse)) -> reddit.EngineMessage,
) -> Result(reddit.EngineResponse, String) {
  let reply = process.new_subject()
  process.send(engine, build(reply))
  case process.receive(reply, engine_timeout_ms) {
    Ok(response) -> Ok(response)
    Error(_) -> Error("Engine timed out")
  }
}

fn decode_body(
  req: request.Request(mist.Connection),
  decoder: decode.Decoder(a),
) -> Result(a, String) {
  case mist.read_body(req, body_limit) {
    Ok(request_with_body) ->
      json.parse_bits(request_with_body.body, decoder)
      |> result.map_error(decode_error_to_string)
    Error(error) -> Error(describe_read_error(error))
  }
}

fn describe_read_error(error: mist.ReadError) -> String {
  case error {
    mist.ExcessBody -> "Request body is too large"
    mist.MalformedBody -> "Request body could not be parsed"
  }
}

fn decode_error_to_string(error: json.DecodeError) -> String {
  case error {
    json.UnexpectedEndOfInput -> "Body ended unexpectedly"
    json.UnexpectedByte(byte) ->
      string.append("Unexpected byte while parsing JSON: ", byte)
    json.UnexpectedSequence(seq) ->
      string.append("Unexpected sequence while parsing JSON: ", seq)
    json.UnableToDecode(errors) -> {
      let descriptions =
        list.map(errors, fn(e) {
          let decode.DecodeError(expected:, found:, path:) = e
          let path_string = list.reverse(path) |> string.join(with: ".")
          "Expected "
          <> expected
          <> ", found "
          <> found
          <> case path_string {
            "" -> ""
            _ -> string.append(" at ", path_string)
          }
        })
      string.join(descriptions, ", ")
    }
  }
}

fn post_to_json(post: reddit.Post) -> Json {
  let reddit.Post(
    id:,
    author:,
    subreddit:,
    content:,
    created_at:,
    upvotes:,
    downvotes:,
    is_repost:,
    original_post_id:,
  ) = post

  json.object([
    #("id", json.string(id)),
    #("author", json.string(author)),
    #("subreddit", json.string(subreddit)),
    #("content", json.string(content)),
    #("created_at", json.int(created_at)),
    #("upvotes", json.int(upvotes)),
    #("downvotes", json.int(downvotes)),
    #("is_repost", json.bool(is_repost)),
    #("original_post_id", case original_post_id {
      "" -> json.null()
      _ -> json.string(original_post_id)
    }),
  ])
}

fn message_to_json(message: reddit.DirectMessage) -> Json {
  let reddit.DirectMessage(
    id:,
    from:,
    to:,
    content:,
    created_at:,
    parent_message_id:,
  ) = message

  json.object([
    #("id", json.string(id)),
    #("from", json.string(from)),
    #("to", json.string(to)),
    #("content", json.string(content)),
    #("created_at", json.int(created_at)),
    #("parent_message_id", json.nullable(parent_message_id, json.string)),
  ])
}

fn ok_response(payload: Json) -> response.Response(mist.ResponseData) {
  respond_with_json(200, payload)
}

fn error_response(
  status: Int,
  message: String,
) -> response.Response(mist.ResponseData) {
  respond_with_json(
    status,
    json.object([
      #("status", json.string("error")),
      #("message", json.string(message)),
    ]),
  )
}

fn respond_with_json(
  status: Int,
  payload: Json,
) -> response.Response(mist.ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "application/json")
  |> response.set_body(
    mist.Bytes(bytes_tree.from_string_tree(json.to_string_tree(payload))),
  )
}
