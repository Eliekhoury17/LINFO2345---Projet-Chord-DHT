-module(saving).
-export([create_folder/2, save_into_file/4]).

create_folder(DirName, Silent) ->
    % NameInArray = io:format("~s", [DirName]),
    % NameInString = lists:flatten(NameInArray),

    case file:make_dir(DirName) of
        ok ->
            if
                not Silent ->
                    io:format("Directory ~s create~n", [DirName]);
                true ->
                    skip
            end;
        {error, Reason} ->
            if
                not Silent ->
                    io:format("Failed to create directory ~s. Reason: ~p~n", [DirName, Reason]);
                true ->
                    skip
            end
    end.

save_into_file(DirName, FileName, Content, Silent) ->
    PathInArray = io_lib:format("~s~s", [DirName, FileName]),
    PathInString = lists:flatten(PathInArray),

    case file:open(PathInString, [write]) of
        {ok, File} ->
            file:write(File, Content),
            file:close(File),
            if
                not Silent ->
                    io:format("Content written into ~s~n", [PathInString]);
                true ->
                    skip
            end;
        {error, Reason} ->
            if
                not Silent ->
                    io:format("Failed to write content into ~s : ~p~n", [PathInString, Reason]);
                true ->
                    skip
            end    
    end.