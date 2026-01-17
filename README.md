# reddit

```sh
gleam run -m reddit_cli <base_url> <command> [args]

# Examples
gleam run -m reddit_cli http://localhost:4000 register alice
gleam run -m reddit_cli http://localhost:4000 create-subreddit alice haskell
gleam run -m reddit_cli http://localhost:4000 post alice haskell "Hello, world!"
gleam run -m reddit_cli http://localhost:4000 feed alice
```

Commands:

- `register <username>`
- `create-subreddit <username> <name>`
- `join-subreddit <username> <name>`
- `leave-subreddit <username> <name>`
- `post <username> <subreddit> <content> [original_post_id]`
- `comment <username> <post_id> <content> [parent_comment_id]`
- `vote-post <username> <post_id> <up|down>`
- `vote-comment <username> <comment_id> <up|down>`
- `feed <username>`
- `send-message <from> <to> <content> [parent_message_id]`
- `messages <username>`

The CLI prints the HTTP status code and the JSON response body so it is easy to
record proof for the milestone video.
