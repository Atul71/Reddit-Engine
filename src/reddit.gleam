import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/string

@external(erlang, "rand", "uniform")
fn rand_uniform(n: Int) -> Int

fn random_integer(min: Int, max: Int) -> Int {
  case min < max {
    True -> {
      let range = max - min
      let value = rand_uniform(range)
      value - 1 + min
    }
    False -> min
  }
}

pub type Account {
  Account(username: String, karma: Int, joined_subreddits: List(String))
}

pub type Post {
  Post(
    id: String,
    author: String,
    subreddit: String,
    content: String,
    created_at: Int,
    upvotes: Int,
    downvotes: Int,
    is_repost: Bool,
    original_post_id: String,
  )
}

pub type Comment {
  Comment(
    id: String,
    author: String,
    post_id: String,
    parent_comment_id: Option(String),
    content: String,
    created_at: Int,
    upvotes: Int,
    downvotes: Int,
  )
}

pub type DirectMessage {
  DirectMessage(
    id: String,
    from: String,
    to: String,
    content: String,
    created_at: Int,
    parent_message_id: Option(String),
  )
}

pub type EngineState {
  EngineState(
    accounts: Dict(String, Account),
    subreddits: Dict(String, List(String)),
    posts: Dict(String, Post),
    comments: Dict(String, Comment),
    messages: Dict(String, DirectMessage),
    post_ids: Int,
    comment_ids: Int,
    message_ids: Int,
  )
}

pub type EngineMessage {
  RegisterAccount(username: String, reply_to: Subject(EngineResponse))
  CreateSubreddit(
    username: String,
    subreddit_name: String,
    reply_to: Subject(EngineResponse),
  )
  JoinSubreddit(
    username: String,
    subreddit_name: String,
    reply_to: Subject(EngineResponse),
  )
  LeaveSubreddit(
    username: String,
    subreddit_name: String,
    reply_to: Subject(EngineResponse),
  )
  CreatePost(
    username: String,
    subreddit_name: String,
    content: String,
    is_repost: Bool,
    original_id: String,
    reply_to: Subject(EngineResponse),
  )
  CreateComment(
    username: String,
    post_id: String,
    parent_comment_id: Option(String),
    content: String,
    reply_to: Subject(EngineResponse),
  )
  Upvote(
    username: String,
    post_id: Option(String),
    comment_id: Option(String),
    reply_to: Subject(EngineResponse),
  )
  Downvote(
    username: String,
    post_id: Option(String),
    comment_id: Option(String),
    reply_to: Subject(EngineResponse),
  )
  GetFeed(username: String, reply_to: Subject(EngineResponse))
  SendMessage(
    from: String,
    to: String,
    content: String,
    parent_message_id: Option(String),
    reply_to: Subject(EngineResponse),
  )
  GetMessages(username: String, reply_to: Subject(EngineResponse))
  ReplyToMessage(
    username: String,
    message_id: String,
    content: String,
    reply_to: Subject(EngineResponse),
  )
  GetState(reply_to: Subject(EngineResponse))
  GetAllPostIds(reply_to: Subject(EngineResponse))
  GetAllCommentIds(reply_to: Subject(EngineResponse))
  GetAllUsernames(reply_to: Subject(EngineResponse))
}

pub type EngineResponse {
  Success(value: String)
  ErrorResponse(message: String)
  FeedResult(posts: List(Post))
  MessagesResult(messages: List(DirectMessage))
  StateResult(state: EngineState)
  PostIdList(ids: List(String))
  CommentIdList(ids: List(String))
  UsernameList(usernames: List(String))
}

pub type ClientActorMessage {
  Start
  ReportStats(reply_to: Subject(ClientState))
}

pub type ClientState {
  ClientState(
    username: String,
    engine: Subject(EngineMessage),
    connected: Bool,
    subreddits: List(String),
    known_post_ids: List(String),
    known_comment_ids: List(String),
    known_usernames: List(String),
    messages_sent: Int,
    posts_created: Int,
    comments_created: Int,
    successful_ops: Int,
    failed_ops: Int,
    disconnected_until: Int,
    current_action: Int,
  )
}

// ============================================================================
// ENGINE CORE FUNCTIONS
// ============================================================================

fn initial_state() -> EngineState {
  EngineState(
    accounts: dict.new(),
    subreddits: dict.new(),
    posts: dict.new(),
    comments: dict.new(),
    messages: dict.new(),
    post_ids: 0,
    comment_ids: 0,
    message_ids: 0,
  )
}

fn register_account(
  username: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.accounts, username) {
    Ok(_) -> Error("Username already exists")
    Error(_) -> {
      let account = Account(username: username, karma: 0, joined_subreddits: [])
      let new_accounts = dict.insert(state.accounts, username, account)
      Ok(EngineState(..state, accounts: new_accounts))
    }
  }
}

fn create_subreddit(
  username: String,
  subreddit_name: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.accounts, username) {
    Error(_) -> Error("User must exist to create a subreddit")
    Ok(account) -> {
      case dict.has_key(state.subreddits, subreddit_name) {
        True -> Error("Subreddit already exists")
        False -> {
          let updated_account =
            Account(
              ..account,
              joined_subreddits: list.append(account.joined_subreddits, [
                subreddit_name,
              ]),
            )
          let new_accounts =
            dict.insert(state.accounts, username, updated_account)
          let new_subreddits =
            dict.insert(state.subreddits, subreddit_name, [username])
          Ok(
            EngineState(
              ..state,
              subreddits: new_subreddits,
              accounts: new_accounts,
            ),
          )
        }
      }
    }
  }
}

fn join_subreddit(
  username: String,
  subreddit_name: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.subreddits, subreddit_name) {
    Error(_) -> Error("Subreddit does not exist")
    Ok(members) -> {
      case list.find(members, fn(name) { name == username }) {
        Ok(_) -> Error("Already a member")
        Error(_) -> {
          let new_members = list.append(members, [username])
          let new_subreddits =
            dict.insert(state.subreddits, subreddit_name, new_members)

          case dict.get(state.accounts, username) {
            Error(_) -> Error("User does not exist")
            Ok(account) -> {
              let updated_account =
                Account(
                  ..account,
                  joined_subreddits: list.append(account.joined_subreddits, [
                    subreddit_name,
                  ]),
                )
              let new_accounts =
                dict.insert(state.accounts, username, updated_account)
              Ok(
                EngineState(
                  ..state,
                  subreddits: new_subreddits,
                  accounts: new_accounts,
                ),
              )
            }
          }
        }
      }
    }
  }
}

fn leave_subreddit(
  username: String,
  subreddit_name: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.subreddits, subreddit_name) {
    Error(_) -> Error("Subreddit does not exist")
    Ok(members) -> {
      let new_members = list.filter(members, fn(name) { name != username })
      let new_subreddits =
        dict.insert(state.subreddits, subreddit_name, new_members)

      case dict.get(state.accounts, username) {
        Error(_) -> Error("User does not exist")
        Ok(account) -> {
          let updated_account =
            Account(
              ..account,
              joined_subreddits: list.filter(
                account.joined_subreddits,
                fn(name) { name != subreddit_name },
              ),
            )
          let new_accounts =
            dict.insert(state.accounts, username, updated_account)
          Ok(
            EngineState(
              ..state,
              subreddits: new_subreddits,
              accounts: new_accounts,
            ),
          )
        }
      }
    }
  }
}

fn create_post(
  username: String,
  subreddit_name: String,
  content: String,
  is_repost: Bool,
  original_id: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.subreddits, subreddit_name) {
    Error(_) -> Error("Subreddit does not exist")
    Ok(members) -> {
      case list.find(members, fn(name) { name == username }) {
        Error(_) -> Error("Not a member of this subreddit")
        Ok(_) -> {
          let post_id = string.append("post_", int.to_string(state.post_ids))
          let timestamp = state.post_ids
          let post =
            Post(
              id: post_id,
              author: username,
              subreddit: subreddit_name,
              content: content,
              created_at: timestamp,
              upvotes: 0,
              downvotes: 0,
              is_repost: is_repost,
              original_post_id: original_id,
            )
          let new_posts = dict.insert(state.posts, post_id, post)
          Ok(
            EngineState(..state, posts: new_posts, post_ids: state.post_ids + 1),
          )
        }
      }
    }
  }
}

fn create_comment(
  username: String,
  post_id: String,
  parent_comment_id: Option(String),
  content: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.posts, post_id) {
    Error(_) -> Error("Post does not exist")
    Ok(_) -> {
      case parent_comment_id {
        Some(parent_id) -> {
          case dict.get(state.comments, parent_id) {
            Error(_) -> Error("Parent comment does not exist")
            Ok(_) ->
              create_comment_helper(
                username,
                post_id,
                parent_comment_id,
                content,
                state,
              )
          }
        }
        None ->
          create_comment_helper(
            username,
            post_id,
            parent_comment_id,
            content,
            state,
          )
      }
    }
  }
}

fn create_comment_helper(
  username: String,
  post_id: String,
  parent_comment_id: Option(String),
  content: String,
  state: EngineState,
) -> Result(EngineState, String) {
  let comment_id = string.append("comment_", int.to_string(state.comment_ids))
  let timestamp = state.comment_ids
  let comment =
    Comment(
      id: comment_id,
      author: username,
      post_id: post_id,
      parent_comment_id: parent_comment_id,
      content: content,
      created_at: timestamp,
      upvotes: 0,
      downvotes: 0,
    )
  let new_comments = dict.insert(state.comments, comment_id, comment)
  Ok(
    EngineState(
      ..state,
      comments: new_comments,
      comment_ids: state.comment_ids + 1,
    ),
  )
}

fn upvote_post(
  _username: String,
  post_id: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.posts, post_id) {
    Error(_) -> Error("Post does not exist")
    Ok(post) -> {
      let updated_post = Post(..post, upvotes: post.upvotes + 1)
      let new_posts = dict.insert(state.posts, post_id, updated_post)
      let new_state =
        update_user_karma(
          post.author,
          1,
          EngineState(..state, posts: new_posts),
        )
      Ok(new_state)
    }
  }
}

fn downvote_post(
  _username: String,
  post_id: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.posts, post_id) {
    Error(_) -> Error("Post does not exist")
    Ok(post) -> {
      let updated_post = Post(..post, downvotes: post.downvotes + 1)
      let new_posts = dict.insert(state.posts, post_id, updated_post)
      let new_state =
        update_user_karma(
          post.author,
          -1,
          EngineState(..state, posts: new_posts),
        )
      Ok(new_state)
    }
  }
}

fn upvote_comment(
  _username: String,
  comment_id: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.comments, comment_id) {
    Error(_) -> Error("Comment does not exist")
    Ok(comment) -> {
      let updated_comment = Comment(..comment, upvotes: comment.upvotes + 1)
      let new_comments =
        dict.insert(state.comments, comment_id, updated_comment)
      let new_state =
        update_user_karma(
          comment.author,
          1,
          EngineState(..state, comments: new_comments),
        )
      Ok(new_state)
    }
  }
}

fn downvote_comment(
  _username: String,
  comment_id: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.comments, comment_id) {
    Error(_) -> Error("Comment does not exist")
    Ok(comment) -> {
      let updated_comment = Comment(..comment, downvotes: comment.downvotes + 1)
      let new_comments =
        dict.insert(state.comments, comment_id, updated_comment)
      let new_state =
        update_user_karma(
          comment.author,
          -1,
          EngineState(..state, comments: new_comments),
        )
      Ok(new_state)
    }
  }
}

fn update_user_karma(
  username: String,
  delta: Int,
  state: EngineState,
) -> EngineState {
  case dict.get(state.accounts, username) {
    Error(_) -> state
    Ok(account) -> {
      let updated_account = Account(..account, karma: account.karma + delta)
      let new_accounts = dict.insert(state.accounts, username, updated_account)
      EngineState(..state, accounts: new_accounts)
    }
  }
}

fn get_feed(username: String, state: EngineState) -> List(Post) {
  case dict.get(state.accounts, username) {
    Error(_) -> []
    Ok(account) -> {
      let all_posts = dict.values(state.posts)
      let subscribed_posts =
        list.filter(all_posts, fn(post) {
          list.any(account.joined_subreddits, fn(sub) { sub == post.subreddit })
        })

      list.sort(subscribed_posts, fn(a, b) {
        let score_a = a.upvotes - a.downvotes
        let score_b = b.upvotes - b.downvotes
        case int.compare(score_a, score_b) {
          order.Lt -> order.Gt
          order.Eq -> order.Eq
          order.Gt -> order.Lt
        }
      })
    }
  }
}

fn send_direct_message(
  from: String,
  to: String,
  content: String,
  parent_message_id: Option(String),
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.accounts, from) {
    Error(_) -> Error("Sender does not exist")
    Ok(_) -> {
      case dict.get(state.accounts, to) {
        Error(_) -> Error("Recipient does not exist")
        Ok(_) -> {
          let message_id =
            string.append("msg_", int.to_string(state.message_ids))
          let timestamp = state.message_ids
          let message =
            DirectMessage(
              id: message_id,
              from: from,
              to: to,
              content: content,
              created_at: timestamp,
              parent_message_id: parent_message_id,
            )
          let new_messages = dict.insert(state.messages, message_id, message)
          Ok(
            EngineState(
              ..state,
              messages: new_messages,
              message_ids: state.message_ids + 1,
            ),
          )
        }
      }
    }
  }
}

fn get_user_messages(
  username: String,
  state: EngineState,
) -> List(DirectMessage) {
  let all_messages = dict.values(state.messages)
  list.filter(all_messages, fn(msg) {
    msg.to == username || msg.from == username
  })
}

fn reply_to_message(
  username: String,
  message_id: String,
  content: String,
  state: EngineState,
) -> Result(EngineState, String) {
  case dict.get(state.messages, message_id) {
    Error(_) -> Error("Message does not exist")
    Ok(original_message) -> {
      let recipient = case original_message.from == username {
        True -> original_message.to
        False -> original_message.from
      }
      send_direct_message(username, recipient, content, Some(message_id), state)
    }
  }
}

// ============================================================================
// ENGINE PROCESS SERVER
// ============================================================================

fn engine_loop(state: EngineState, subject: Subject(EngineMessage)) -> Nil {
  let message = process.receive_forever(subject)
  let new_state = handle_message(state, message)
  engine_loop(new_state, subject)
}

fn handle_message(state: EngineState, message: EngineMessage) -> EngineState {
  case message {
    RegisterAccount(username, reply_to) -> {
      case register_account(username, state) {
        Ok(new_state) -> {
          process.send(reply_to, Success("Account registered"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    CreateSubreddit(username, subreddit_name, reply_to) -> {
      case create_subreddit(username, subreddit_name, state) {
        Ok(new_state) -> {
          process.send(reply_to, Success("Subreddit created"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    JoinSubreddit(username, subreddit_name, reply_to) -> {
      case join_subreddit(username, subreddit_name, state) {
        Ok(new_state) -> {
          process.send(reply_to, Success("Joined subreddit"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    LeaveSubreddit(username, subreddit_name, reply_to) -> {
      case leave_subreddit(username, subreddit_name, state) {
        Ok(new_state) -> {
          process.send(reply_to, Success("Left subreddit"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    CreatePost(
      username,
      subreddit_name,
      content,
      is_repost,
      original_id,
      reply_to,
    ) -> {
      case
        create_post(
          username,
          subreddit_name,
          content,
          is_repost,
          original_id,
          state,
        )
      {
        Ok(new_state) -> {
          process.send(reply_to, Success("Post created"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    CreateComment(username, post_id, parent_comment_id, content, reply_to) -> {
      case
        create_comment(username, post_id, parent_comment_id, content, state)
      {
        Ok(new_state) -> {
          process.send(reply_to, Success("Comment created"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    Upvote(username, post_id, comment_id, reply_to) -> {
      case post_id {
        Some(id) -> {
          case upvote_post(username, id, state) {
            Ok(new_state) -> {
              process.send(reply_to, Success("Post upvoted"))
              new_state
            }
            Error(err) -> {
              process.send(reply_to, ErrorResponse(err))
              state
            }
          }
        }
        None -> {
          case comment_id {
            Some(id) -> {
              case upvote_comment(username, id, state) {
                Ok(new_state) -> {
                  process.send(reply_to, Success("Comment upvoted"))
                  new_state
                }
                Error(err) -> {
                  process.send(reply_to, ErrorResponse(err))
                  state
                }
              }
            }
            None -> {
              process.send(
                reply_to,
                ErrorResponse("Must specify post_id or comment_id"),
              )
              state
            }
          }
        }
      }
    }

    Downvote(username, post_id, comment_id, reply_to) -> {
      case post_id {
        Some(id) -> {
          case downvote_post(username, id, state) {
            Ok(new_state) -> {
              process.send(reply_to, Success("Post downvoted"))
              new_state
            }
            Error(err) -> {
              process.send(reply_to, ErrorResponse(err))
              state
            }
          }
        }
        None -> {
          case comment_id {
            Some(id) -> {
              case downvote_comment(username, id, state) {
                Ok(new_state) -> {
                  process.send(reply_to, Success("Comment downvoted"))
                  new_state
                }
                Error(err) -> {
                  process.send(reply_to, ErrorResponse(err))
                  state
                }
              }
            }
            None -> {
              process.send(
                reply_to,
                ErrorResponse("Must specify post_id or comment_id"),
              )
              state
            }
          }
        }
      }
    }

    GetFeed(username, reply_to) -> {
      let feed = get_feed(username, state)
      process.send(reply_to, FeedResult(feed))
      state
    }

    SendMessage(from, to, content, parent_message_id, reply_to) -> {
      case send_direct_message(from, to, content, parent_message_id, state) {
        Ok(new_state) -> {
          process.send(reply_to, Success("Message sent"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    GetMessages(username, reply_to) -> {
      let messages = get_user_messages(username, state)
      process.send(reply_to, MessagesResult(messages))
      state
    }

    ReplyToMessage(username, message_id, content, reply_to) -> {
      case reply_to_message(username, message_id, content, state) {
        Ok(new_state) -> {
          process.send(reply_to, Success("Message replied"))
          new_state
        }
        Error(err) -> {
          process.send(reply_to, ErrorResponse(err))
          state
        }
      }
    }

    GetState(reply_to) -> {
      process.send(reply_to, StateResult(state))
      state
    }

    GetAllPostIds(reply_to) -> {
      let post_ids = dict.keys(state.posts)
      process.send(reply_to, PostIdList(post_ids))
      state
    }

    GetAllCommentIds(reply_to) -> {
      let comment_ids = dict.keys(state.comments)
      process.send(reply_to, CommentIdList(comment_ids))
      state
    }

    GetAllUsernames(reply_to) -> {
      let usernames = dict.keys(state.accounts)
      process.send(reply_to, UsernameList(usernames))
      state
    }
  }
}

pub fn start_engine() -> Subject(EngineMessage) {
  let reply_subject = process.new_subject()

  let _ =
    process.spawn(fn() {
      let engine_subject = process.new_subject()
      process.send(reply_subject, engine_subject)
      engine_loop(initial_state(), engine_subject)
    })

  process.receive_forever(reply_subject)
}

// ============================================================================
// ZIPF DISTRIBUTION HELPER
// ============================================================================

fn zipf_rank(index: Int) -> Float {
  let rank = int.to_float(index + 1)
  1.0 /. rank
}

fn zipf_distribution(count: Int) -> List(Float) {
  list.range(0, count - 1)
  |> list.map(fn(i) { zipf_rank(i) })
}

fn normalize_zipf(probs: List(Float)) -> List(Float) {
  let sum = list.fold(probs, 0.0, fn(acc, prob) { acc +. prob })
  list.map(probs, fn(prob) { prob /. sum })
}

fn select_by_probability(
  random: Float,
  probs: List(Float),
  items: List(String),
) -> String {
  select_by_probability_helper(random, 0.0, probs, items, "")
}

fn select_by_probability_helper(
  random: Float,
  accum: Float,
  probs: List(Float),
  items: List(String),
  default: String,
) -> String {
  case probs, items {
    [prob, ..rest_probs], [item, ..rest_items] -> {
      let new_accum = accum +. prob
      case random <=. new_accum {
        True -> item
        False ->
          select_by_probability_helper(
            random,
            new_accum,
            rest_probs,
            rest_items,
            item,
          )
      }
    }
    _, [item, ..] -> item
    _, _ -> default
  }
}

fn list_at(lst: List(a), index: Int) -> Result(a, Nil) {
  case index < 0 {
    True -> Error(Nil)
    False -> list_at_helper(lst, index)
  }
}

fn list_at_helper(lst: List(a), index: Int) -> Result(a, Nil) {
  case lst {
    [] -> Error(Nil)
    [first, ..rest] ->
      case index {
        0 -> Ok(first)
        _ -> list_at_helper(rest, index - 1)
      }
  }
}

// ============================================================================
// CLIENT ACTOR PROCESS
// ============================================================================

fn client_actor_loop(
  state: ClientState,
  subject: Subject(ClientActorMessage),
  actions: Int,
  subreddit_popularity: Dict(String, Int),
) -> Nil {
  let message = process.receive_forever(subject)

  case message {
    Start -> {
      // Run all actions
      let final_state = simulate_client(state, actions, subreddit_popularity)
      // Continue loop with final state
      client_actor_loop(final_state, subject, actions, subreddit_popularity)
    }

    ReportStats(reply_to) -> {
      // Send back stats
      process.send(reply_to, state)
      // Continue loop
      client_actor_loop(state, subject, actions, subreddit_popularity)
    }
  }
}

fn spawn_client_actor(
  username: String,
  engine: Subject(EngineMessage),
  subreddits: List(String),
  all_usernames: List(String),
  actions: Int,
  subreddit_popularity: Dict(String, Int),
) -> Subject(ClientActorMessage) {
  let reply_subject = process.new_subject()

  let _ =
    process.spawn(fn() {
      let actor_subject = process.new_subject()
      process.send(reply_subject, actor_subject)

      let initial_state =
        ClientState(
          username: username,
          engine: engine,
          connected: True,
          subreddits: subreddits,
          known_post_ids: [],
          known_comment_ids: [],
          known_usernames: all_usernames,
          messages_sent: 0,
          posts_created: 0,
          comments_created: 0,
          successful_ops: 0,
          failed_ops: 0,
          disconnected_until: 0,
          current_action: 0,
        )

      client_actor_loop(
        initial_state,
        actor_subject,
        actions,
        subreddit_popularity,
      )
    })

  process.receive_forever(reply_subject)
}

fn simulate_client(
  client_state: ClientState,
  actions: Int,
  subreddit_popularity: Dict(String, Int),
) -> ClientState {
  list.range(0, actions - 1)
  |> list.fold(client_state, fn(acc, action_num) {
    let updated_state = ClientState(..acc, current_action: action_num)
    case updated_state.connected {
      False -> {
        case action_num >= updated_state.disconnected_until {
          True ->
            simulate_action(
              ClientState(..updated_state, connected: True),
              subreddit_popularity,
            )
          False -> updated_state
        }
      }
      True -> simulate_action(updated_state, subreddit_popularity)
    }
  })
}

fn simulate_action(
  state: ClientState,
  subreddit_popularity: Dict(String, Int),
) -> ClientState {
  let action = random_integer(0, 10)

  case action {
    0 -> simulate_register(state)
    1 -> simulate_join_subreddit(state)
    2 -> simulate_create_post(state, subreddit_popularity)
    3 -> simulate_comment(state)
    4 -> simulate_vote(state)
    5 -> simulate_get_feed(state)
    6 -> simulate_send_message(state)
    7 -> simulate_get_messages(state)
    8 -> simulate_disconnect(state)
    _ -> state
  }
}

fn simulate_register(state: ClientState) -> ClientState {
  let reply_to = process.new_subject()
  process.send(state.engine, RegisterAccount(state.username, reply_to))

  case process.receive(reply_to, 100) {
    Ok(Success(_)) ->
      ClientState(..state, successful_ops: state.successful_ops + 1)
    _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
  }
}

fn simulate_join_subreddit(state: ClientState) -> ClientState {
  let subreddit_name =
    string.append("subreddit_", int.to_string(random_integer(0, 10)))
  let reply_to = process.new_subject()
  process.send(
    state.engine,
    JoinSubreddit(state.username, subreddit_name, reply_to),
  )

  case process.receive(reply_to, 100) {
    Ok(Success(_)) ->
      ClientState(
        ..state,
        subreddits: list.append(state.subreddits, [subreddit_name]),
        successful_ops: state.successful_ops + 1,
      )
    _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
  }
}

fn simulate_create_post(
  state: ClientState,
  subreddit_popularity: Dict(String, Int),
) -> ClientState {
  case state.subreddits {
    [] -> ClientState(..state, failed_ops: state.failed_ops + 1)
    _ -> {
      let subreddit_index = random_integer(0, list.length(state.subreddits))
      let subreddit = case list_at(state.subreddits, subreddit_index) {
        Ok(s) -> s
        Error(_) -> ""
      }

      case subreddit {
        "" -> ClientState(..state, failed_ops: state.failed_ops + 1)
        _ -> {
          let member_count = case dict.get(subreddit_popularity, subreddit) {
            Ok(count) -> count
            Error(_) -> 1
          }

          let post_threshold = case member_count > 10 {
            True -> 40
            False ->
              case member_count > 5 {
                True -> 25
                False -> 15
              }
          }

          case random_integer(0, 100) < post_threshold {
            False -> state
            True -> {
              let content = string.append("Post content from ", state.username)

              let is_repost = case state.known_post_ids != [] {
                True -> random_integer(0, 100) < 20
                False -> False
              }

              let original_id = case is_repost {
                True -> {
                  let idx = random_integer(0, list.length(state.known_post_ids))
                  case list_at(state.known_post_ids, idx) {
                    Ok(id) -> id
                    Error(_) -> ""
                  }
                }
                False -> ""
              }

              let reply_to = process.new_subject()
              process.send(
                state.engine,
                CreatePost(
                  state.username,
                  subreddit,
                  content,
                  is_repost,
                  original_id,
                  reply_to,
                ),
              )

              case process.receive(reply_to, 100) {
                Ok(Success(_)) -> {
                  let ids_reply = process.new_subject()
                  process.send(state.engine, GetAllPostIds(ids_reply))
                  let new_post_ids = case process.receive(ids_reply, 100) {
                    Ok(PostIdList(ids)) -> ids
                    _ -> state.known_post_ids
                  }
                  ClientState(
                    ..state,
                    posts_created: state.posts_created + 1,
                    successful_ops: state.successful_ops + 1,
                    known_post_ids: new_post_ids,
                  )
                }
                _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
              }
            }
          }
        }
      }
    }
  }
}

fn simulate_comment(state: ClientState) -> ClientState {
  case state.known_post_ids {
    [] -> ClientState(..state, failed_ops: state.failed_ops + 1)
    _ -> {
      let post_idx = random_integer(0, list.length(state.known_post_ids))
      let post_id = case list_at(state.known_post_ids, post_idx) {
        Ok(id) -> id
        Error(_) -> ""
      }

      case post_id {
        "" -> ClientState(..state, failed_ops: state.failed_ops + 1)
        _ -> {
          let content = string.append("Comment from ", state.username)

          let parent_id = case state.known_comment_ids != [] {
            True ->
              case random_integer(0, 100) < 30 {
                True -> {
                  let idx =
                    random_integer(0, list.length(state.known_comment_ids))
                  case list_at(state.known_comment_ids, idx) {
                    Ok(id) -> Some(id)
                    Error(_) -> None
                  }
                }
                False -> None
              }
            False -> None
          }

          let reply_to = process.new_subject()
          process.send(
            state.engine,
            CreateComment(state.username, post_id, parent_id, content, reply_to),
          )

          case process.receive(reply_to, 100) {
            Ok(Success(_)) -> {
              let ids_reply = process.new_subject()
              process.send(state.engine, GetAllCommentIds(ids_reply))
              let new_comment_ids = case process.receive(ids_reply, 100) {
                Ok(CommentIdList(ids)) -> ids
                _ -> state.known_comment_ids
              }
              ClientState(
                ..state,
                comments_created: state.comments_created + 1,
                successful_ops: state.successful_ops + 1,
                known_comment_ids: new_comment_ids,
              )
            }
            _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
          }
        }
      }
    }
  }
}

fn simulate_vote(state: ClientState) -> ClientState {
  let vote_post = random_integer(0, 100) < 70

  case vote_post {
    True -> {
      case state.known_post_ids {
        [] -> ClientState(..state, failed_ops: state.failed_ops + 1)
        _ -> {
          let idx = random_integer(0, list.length(state.known_post_ids))
          let post_id = case list_at(state.known_post_ids, idx) {
            Ok(id) -> Some(id)
            Error(_) -> None
          }

          let is_upvote = random_integer(0, 100) < 70
          let reply_to = process.new_subject()

          case is_upvote {
            True ->
              process.send(
                state.engine,
                Upvote(state.username, post_id, None, reply_to),
              )
            False ->
              process.send(
                state.engine,
                Downvote(state.username, post_id, None, reply_to),
              )
          }

          case process.receive(reply_to, 100) {
            Ok(Success(_)) ->
              ClientState(..state, successful_ops: state.successful_ops + 1)
            _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
          }
        }
      }
    }
    False -> {
      case state.known_comment_ids {
        [] -> ClientState(..state, failed_ops: state.failed_ops + 1)
        _ -> {
          let idx = random_integer(0, list.length(state.known_comment_ids))
          let comment_id = case list_at(state.known_comment_ids, idx) {
            Ok(id) -> Some(id)
            Error(_) -> None
          }

          let is_upvote = random_integer(0, 100) < 70
          let reply_to = process.new_subject()

          case is_upvote {
            True ->
              process.send(
                state.engine,
                Upvote(state.username, None, comment_id, reply_to),
              )
            False ->
              process.send(
                state.engine,
                Downvote(state.username, None, comment_id, reply_to),
              )
          }

          case process.receive(reply_to, 100) {
            Ok(Success(_)) ->
              ClientState(..state, successful_ops: state.successful_ops + 1)
            _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
          }
        }
      }
    }
  }
}

fn simulate_get_feed(state: ClientState) -> ClientState {
  let reply_to = process.new_subject()
  process.send(state.engine, GetFeed(state.username, reply_to))

  case process.receive(reply_to, 100) {
    Ok(FeedResult(_)) ->
      ClientState(..state, successful_ops: state.successful_ops + 1)
    _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
  }
}

fn simulate_send_message(state: ClientState) -> ClientState {
  case list.length(state.known_usernames) {
    0 -> ClientState(..state, failed_ops: state.failed_ops + 1)
    1 -> ClientState(..state, failed_ops: state.failed_ops + 1)
    _ -> {
      let idx = random_integer(0, list.length(state.known_usernames))
      let recipient = case list_at(state.known_usernames, idx) {
        Ok(name) -> name
        Error(_) -> ""
      }

      case recipient == state.username || recipient == "" {
        True -> ClientState(..state, failed_ops: state.failed_ops + 1)
        False -> {
          let content = string.append("Message from ", state.username)
          let reply_to = process.new_subject()

          process.send(
            state.engine,
            SendMessage(state.username, recipient, content, None, reply_to),
          )

          case process.receive(reply_to, 100) {
            Ok(Success(_)) ->
              ClientState(
                ..state,
                messages_sent: state.messages_sent + 1,
                successful_ops: state.successful_ops + 1,
              )
            _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
          }
        }
      }
    }
  }
}

fn simulate_get_messages(state: ClientState) -> ClientState {
  let reply_to = process.new_subject()
  process.send(state.engine, GetMessages(state.username, reply_to))

  case process.receive(reply_to, 100) {
    Ok(MessagesResult(_)) ->
      ClientState(..state, successful_ops: state.successful_ops + 1)
    _ -> ClientState(..state, failed_ops: state.failed_ops + 1)
  }
}

fn simulate_disconnect(state: ClientState) -> ClientState {
  let disconnect_duration = random_integer(5, 15)
  ClientState(
    ..state,
    connected: False,
    disconnected_until: state.current_action + disconnect_duration,
  )
}

pub type PerformanceStats {
  PerformanceStats(
    total_users: Int,
    total_subreddits: Int,
    total_posts: Int,
    total_comments: Int,
    total_messages: Int,
    total_operations: Int,
    time_elapsed_ms: Int,
    successful_operations: Int,
    failed_operations: Int,
    operations_per_second: Float,
  )
}

fn measure_performance(
  engine: Subject(EngineMessage),
  client_results: List(ClientState),
  elapsed_ms: Int,
) -> PerformanceStats {
  let reply_to = process.new_subject()
  process.send(engine, GetState(reply_to))

  let state = case process.receive(reply_to, 5000) {
    Ok(StateResult(s)) -> s
    _ -> initial_state()
  }

  let total_successful =
    list.fold(client_results, 0, fn(acc, client) { acc + client.successful_ops })

  let total_failed =
    list.fold(client_results, 0, fn(acc, client) { acc + client.failed_ops })

  let ops_per_second = case elapsed_ms > 0 {
    True -> {
      let seconds = int.to_float(elapsed_ms) /. 1000.0
      int.to_float(total_successful) /. seconds
    }
    False -> 0.0
  }

  PerformanceStats(
    total_users: dict.size(state.accounts),
    total_subreddits: dict.size(state.subreddits),
    total_posts: dict.size(state.posts),
    total_comments: dict.size(state.comments),
    total_messages: dict.size(state.messages),
    total_operations: dict.size(state.posts)
      + dict.size(state.comments)
      + dict.size(state.messages),
    time_elapsed_ms: elapsed_ms,
    successful_operations: total_successful,
    failed_operations: total_failed,
    operations_per_second: ops_per_second,
  )
}

fn print_performance_stats(stats: PerformanceStats) -> Nil {
  // io.println("\n=== PERFORMANCE STATISTICS ===")
  io.println(string.append("Total Users: ", int.to_string(stats.total_users)))
  io.println(string.append(
    "Total Subreddits: ",
    int.to_string(stats.total_subreddits),
  ))
  io.println(string.append("Total Posts: ", int.to_string(stats.total_posts)))
  io.println(string.append(
    "Total Comments: ",
    int.to_string(stats.total_comments),
  ))
  io.println(string.append(
    "Total Messages: ",
    int.to_string(stats.total_messages),
  ))
  io.println(string.append(
    "Total Operations: ",
    int.to_string(stats.total_operations),
  ))
  // io.println(string.append(
  //   "Successful Operations: ",
  //   int.to_string(stats.successful_operations),
  // ))
  // io.println(string.append(
  //   "Failed Operations: ",
  //   int.to_string(stats.failed_operations),
  // ))

  let _success_rate = case
    stats.successful_operations + stats.failed_operations
  {
    0 -> 0.0
    total -> {
      let rate =
        int.to_float(stats.successful_operations)
        /. int.to_float(total)
        *. 100.0
      rate
    }
  }
  // io.println(string.append(
  //   "Success Rate: ",
  //   string.append(float.to_string(success_rate), "%"),
  // ))
  io.println(string.append(
    "Operations/Second: ",
    float.to_string(stats.operations_per_second),
  ))
  io.println(string.append(
    "Time Elapsed (ms): ",
    int.to_string(stats.time_elapsed_ms),
  ))
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: Int) -> Int

fn get_time_ms() -> Int {
  monotonic_time(1_000_000) / 1000
}

pub fn main() -> Nil {
  let start_time = get_time_ms()
  let engine = start_engine()
  //io.println("Engine started")

  let num_clients = 150
  let actions_per_client = 30
  let num_subreddits = 40

  io.println(string.append(
    "Starting ",
    string.append(int.to_string(num_clients), " client simulators"),
  ))

  // Initialize admin and subreddits
  let reply_to = process.new_subject()
  process.send(engine, RegisterAccount("admin", reply_to))
  let _ = process.receive(reply_to, 1000)

  let subreddit_names =
    list.range(0, num_subreddits - 1)
    |> list.map(fn(i) { string.append("subreddit_", int.to_string(i)) })

  list.each(subreddit_names, fn(sub_name) {
    let reply_to2 = process.new_subject()
    process.send(engine, CreateSubreddit("admin", sub_name, reply_to2))
    let _ = process.receive(reply_to2, 1000)
    Nil
  })

  let zipf_probs = zipf_distribution(num_subreddits) |> normalize_zipf

  //io.println("Registering users and joining subreddits...")

  // Register all users and have them join subreddits
  let registered_clients =
    list.range(0, num_clients - 1)
    |> list.map(fn(i) {
      let username = string.append("user_", int.to_string(i))

      let reply_reg = process.new_subject()
      process.send(engine, RegisterAccount(username, reply_reg))
      let _ = process.receive(reply_reg, 1000)

      let num_joins = random_integer(1, 6)

      let joined_subreddits =
        list.range(0, num_joins - 1)
        |> list.fold([], fn(acc, _) {
          let random_val = int.to_float(random_integer(0, 1000)) /. 1000.0
          let selected =
            select_by_probability(random_val, zipf_probs, subreddit_names)

          case list.find(acc, fn(name) { name == selected }) {
            Ok(_) -> acc
            Error(_) -> {
              let reply_join = process.new_subject()
              process.send(
                engine,
                JoinSubreddit(username, selected, reply_join),
              )
              case process.receive(reply_join, 1000) {
                Ok(Success(_)) -> list.append(acc, [selected])
                _ -> acc
              }
            }
          }
        })

      #(username, joined_subreddits)
    })

  // Calculate subreddit popularity
  let subreddit_popularity =
    list.fold(registered_clients, dict.new(), fn(pop_dict, client_tuple) {
      let #(_, subs) = client_tuple
      list.fold(subs, pop_dict, fn(pd, sub) {
        let current = case dict.get(pd, sub) {
          Ok(count) -> count
          Error(_) -> 0
        }
        dict.insert(pd, sub, current + 1)
      })
    })

  let all_usernames =
    list.map(registered_clients, fn(ct) {
      let #(username, _) = ct
      username
    })

  //io.println("Spawning client actors...")

  // Spawn all client actors as separate processes
  let client_actors =
    registered_clients
    |> list.map(fn(client_tuple) {
      let #(username, joined_subreddits) = client_tuple
      spawn_client_actor(
        username,
        engine,
        joined_subreddits,
        all_usernames,
        actions_per_client,
        subreddit_popularity,
      )
    })

  //io.println("Starting concurrent simulation...")

  // Start all actors concurrently
  list.each(client_actors, fn(actor) {
    process.send(actor, Start)
    Nil
  })

  // Wait a bit for all to finish (30 actions * 100ms timeout per action)
  process.sleep(5000)

  //io.println("Collecting results...")

  // Collect stats from all actors
  let client_results =
    list.map(client_actors, fn(actor) {
      let reply_to = process.new_subject()
      process.send(actor, ReportStats(reply_to))
      case process.receive(reply_to, 1000) {
        Ok(state) -> state
        Error(_) ->
          ClientState(
            username: "unknown",
            engine: engine,
            connected: False,
            subreddits: [],
            known_post_ids: [],
            known_comment_ids: [],
            known_usernames: [],
            messages_sent: 0,
            posts_created: 0,
            comments_created: 0,
            successful_ops: 0,
            failed_ops: 0,
            disconnected_until: 0,
            current_action: 0,
          )
      }
    })

  // Print summary
  // list.take(client_results, 10)
  // |> list.each(fn(final_state) {
  //   io.println(string.append("Client ", final_state.username))
  //   io.println(string.append(
  //     "  Posts: ",
  //     int.to_string(final_state.posts_created),
  //   ))
  //   io.println(string.append(
  //     "  Comments: ",
  //     int.to_string(final_state.comments_created),
  //   ))
  //   io.println(string.append(
  //     "  Messages: ",
  //     int.to_string(final_state.messages_sent),
  //   ))
  //   io.println(string.append(
  //     "  Subreddits: ",
  //     int.to_string(list.length(final_state.subreddits)),
  //   ))
  //   io.println(string.append(
  //     "  Success Rate: ",
  //     string.append(
  //       int.to_string(final_state.successful_ops),
  //       string.append(
  //         "/",
  //         int.to_string(final_state.successful_ops + final_state.failed_ops),
  //       ),
  //     ),
  //   ))
  // })

  let end_time = get_time_ms()
  let elapsed = end_time - start_time

  let stats = measure_performance(engine, client_results, elapsed)
  print_performance_stats(stats)

  //io.println("\n=== SUBREDDIT POPULARITY (Top 10) ===")
  let popularity_list = dict.to_list(subreddit_popularity)
  let _sorted_popularity =
    list.sort(popularity_list, fn(a, b) {
      let #(_, count_a) = a
      let #(_, count_b) = b
      case int.compare(count_a, count_b) {
        order.Lt -> order.Gt
        order.Eq -> order.Eq
        order.Gt -> order.Lt
      }
    })

  // list.take(sorted_popularity, 10)
  // |> list.each(fn(sub_tuple) {
  //   let #(name, count) = sub_tuple
  //   io.println(string.append(
  //     name,
  //     string.append(": ", string.append(int.to_string(count), " members")),
  //   ))
  // })

  // io.println("\nSimulation complete!")
  io.println(string.append(
    "Total processes: ",
    string.append(
      int.to_string(1 + num_clients),
      string.append(
        "  (1 engine process + ",
        string.append(int.to_string(num_clients), " client actor processes)"),
      ),
    ),
  ))
}
