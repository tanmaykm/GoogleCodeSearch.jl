"""
A context (`Ctx`) instance encapsulates the index location and provides a useful way to split indexing across multiple indices. It also holds a lock to handle it being called across tasks.
"""
struct Ctx
    store::String       # directory where to store indices
    resolver::Any       # function to determine index filename (as subpath of store) if needed
    lock::ReentrantLock # lock (`withenv` not task safe, could have used per index lock if it were)

    function Ctx(; store::String=joinpath(homedir(), ".googlecodesearchjl"), resolver=(ctx,inpath)->joinpath(ctx.store,"index"))
        mkpath(store)
        new(store, resolver, ReentrantLock())
    end
end

show(io::IO, ctx::Ctx) = print(io, "GoogleCodeSearch.Ctx(store=\"" * ctx.store * "\")")

function readcmd_with_index(ctx::Ctx, cmd::Cmd, idxpath::String)
    pipe_out = Pipe()
    pipe_err = Pipe()
    proc = lock(ctx.lock) do
        withenv("CSEARCHINDEX"=>idxpath) do
            run(pipeline(cmd, stdout=pipe_out, stderr=pipe_err), wait=false)
        end
    end

    success = false
    stdout_buff = PipeBuffer()
    stderr_buff = PipeBuffer()

    @sync begin
        @async begin
            wait(proc)
            success = (proc.exitcode == 0)
            close(Base.pipe_writer(pipe_out))
            close(Base.pipe_writer(pipe_err))
        end
        @async begin
            reader_out = Base.pipe_reader(pipe_out)
            while !(eof(reader_out))
                write(stdout_buff, readavailable(reader_out))
            end
        end
        @async begin
            reader_err = Base.pipe_reader(pipe_err)
            while !(eof(reader_err))
                write(stderr_buff, readavailable(reader_err))
            end
        end
    end
    success, readlines(stdout_buff; keep=true), readlines(stderr_buff; keep=true)
end

"""
Returns a list of index files that are being used in this context.
"""
indices(ctx::Ctx) = map(x->joinpath(ctx.store,x), readdir(ctx.store))

"""
Clears all indices. Fresh index will be created on next call to `index`.
"""
function clear_indices(ctx::Ctx)
    for file in indices(ctx)
        rm(file; force=true)
    end
    nothing
end

"""
Returns a list of paths that have been indexed.
"""
function paths_indexed(ctx::Ctx)
    paths = Set{String}()
    cindex() do cindex_path
        cmd = Cmd([cindex_path, "-list"])
        for idxpath in indices(ctx)
            success, out, err = readcmd_with_index(ctx, cmd, idxpath)
            if success
                for m in out
                    push!(paths, strip(m))
                end
            else
                error("error reading index $idxpath")
            end
        end
    end
    paths
end

"""
Index paths by calling the index method. While indexing, ensure paths are sorted such that paths appearing later are not substrings of those earlier. Otherwise, the earlier indexed entries are erased (strange behavior of `cindex`).
"""
function index(ctx::Ctx, path::String)
    success, out, err = cindex() do cindex_path
        cmd = Cmd([cindex_path, path])
        readcmd_with_index(ctx, cmd, ctx.resolver(ctx,path))
    end
    success
end

function index(ctx::Ctx, paths::Vector{String})
    idxpaths = Dict{String,Vector{String}}()
    for path in paths
        idxpath = ctx.resolver(ctx,path)
        paths = get!(()->String[], idxpaths, idxpath)
        push!(paths, path)
    end
    results = Bool[]
    for (idxpath, paths) in idxpaths
        success, out, err = cindex() do cindex_path
            cmd = Cmd(append!([cindex_path], paths))
            readcmd_with_index(ctx, cmd, idxpath)
        end
        push!(results, success)
    end
    results
end

"""
Search by calling the search method with a regex pattern to search for. Optionally pass the following parameters:
- `ignorecase`: boolean, whether to ignore case during search (default false)
- `pathfilter`: a regular expression string to restrict search only to matching paths

The search method returns a vector of named tuples, each describing a match.
- `file`: path that matched
- `line`: line number therein that matched
- `text`: text that matched
"""
function search(ctx::Ctx, pattern::String; ignorecase::Bool=false, pathfilter::Union{Nothing,String}=nothing, maxresults::Int=20)
    results = Vector{NamedTuple{(:file,:line,:text),Tuple{String,Int,String}}}()
    csearch() do csearch_path
        cmdparts = [csearch_path]
        if pathfilter !== nothing
            push!(cmdparts, "-f")
            push!(cmdparts, pathfilter)
        end
        ignorecase && push!(cmdparts, "-i")
        push!(cmdparts, "-n")
        push!(cmdparts, pattern)
        cmd = Cmd(cmdparts)
        for idx in readdir(ctx.store)
            idxpath = joinpath(ctx.store, idx)
            success, out, err = readcmd_with_index(ctx, cmd, idxpath)
            if success
                for s in out
                    s = strip(s)
                    ( isempty(s) || !startswith(s, "/")) && continue
                    parts = split(s, ':'; limit=3)
                    (length(parts) != 3) && continue
                    (length(results) > maxresults) && (return results)
                    try
                        push!(results, (file=String(parts[1]), line=parse(Int,parts[2]), text=String(parts[3])))
                    catch ex
                        @info "Exception: ",ex
                    end
                end
            end
        end
    end
    results
end
