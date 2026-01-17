-module(reddit_cli_ffi).
-export([argv/0]).

argv() ->
    [unicode:characters_to_binary(Arg) || Arg <- init:get_plain_arguments()].

