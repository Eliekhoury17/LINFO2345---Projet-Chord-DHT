-module(saving).
-export([create_folder/1, save_into_file/3]).

create_folder(DirName) ->
    % NameInArray = io:format("~s", [DirName]),
    % NameInString = lists:flatten(NameInArray),

    case file:make_dir(DirName) of
        ok ->
            
            io:format("Directory ~s create~n", [DirName]);
        {error, Reason} ->
            io:format("Failed to create directory ~s. Reason: ~p~n", [DirName, Reason])
    end.

save_into_file(DirName, FileName, Content) ->
    PathInArray = io_lib:format("~s~s", [DirName, FileName]),
    PathInString = lists:flatten(PathInArray),

    case file:open(PathInString, [write]) of
        {ok, File} ->
            file:write(File, Content),
            file:close(File),
            io:format("Content written into ~s~n", [PathInString]);
        {error, Reason} ->
            io:format("Failed to write content into ~s : ~p~n", [PathInString, Reason])
    end.