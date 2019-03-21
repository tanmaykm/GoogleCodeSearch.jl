module GoogleCodeSearch

using BinaryProvider

import Base: show
export Ctx, index, search, show, indices, clear_indices, paths_indexed

const depsjl_path = joinpath(dirname(@__FILE__), "..", "deps", "deps.jl")
if !isfile(depsjl_path)
    error("Blosc not installed properly, run Pkg.build(\"ZMQ\"), restart Julia and try again")
end
include(depsjl_path)

struct Ctx
    store::String       # directory where to store indices
    resolver::Any       # function to determine index filename (as subpath of store) if needed
    lock::ReentrantLock # lock (`withenv` not task safe, could have used per index lock if it were)

    function Ctx(; store::String=joinpath(homedir(), ".googlecodesearchjl"), resolver=(ctx,inpath)->joinpath(ctx.store,"index"))
        new(store, resolver, ReentrantLock())
    end
end

show(io::IO, ctx::Ctx) = print(io, "GoogleCodeSearch.Ctx(store=\"" * ctx.store * "\")")

function readcmd_with_index(ctx::Ctx, cmd::Cmd, idxpath::String)
    oc = lock(ctx.lock) do
        withenv("CSEARCHINDEX"=>idxpath) do
            OutputCollector(cmd)
        end
    end
    success = wait(oc)
    success, oc.stdout_linestream.lines, oc.stderr_linestream.lines
end

indices(ctx::Ctx) = map(x->joinpath(ctx.store,x), readdir(ctx.store))

function clear_indices(ctx::Ctx)
    for file in indices(ctx)
        rm(file; force=true)
    end
    nothing
end

function paths_indexed(ctx::Ctx)
    paths = Set{String}()
    cmd = Cmd([cindex, "-list"])
    for idxpath in indices(ctx)
        success, out, err = readcmd_with_index(ctx, cmd, idxpath)
        if success
            for (t,m) in out
                push!(paths, strip(m))
            end
        else
            error("error reading index $idxpath")
        end
    end
    paths
end

function index(ctx::Ctx, path::String)
    cmd = Cmd([cindex, path])
    success, out, err = readcmd_with_index(ctx, cmd, ctx.resolver(ctx,path))
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
        cmd = Cmd(append!([cindex], paths))
        success, out, err = readcmd_with_index(ctx, cmd, idxpath)
        push!(results, success)
    end
    results
end

function search(ctx::Ctx, pattern::String; ignorecase::Bool=false, pathfilter::Union{Nothing,String}=nothing)
    cmdparts = [csearch]
    if pathfilter !== nothing
        push!(cmdparts, "-f")
        push!(cmdparts, pathfilter)
    end
    ignorecase && push!(cmdparts, "-i")
    push!(cmdparts, "-n")
    push!(cmdparts, pattern)
    cmd = Cmd(cmdparts)
    results = Vector{NamedTuple{(:file,:line,:text),Tuple{String,Int,String}}}()
    for idx in readdir(ctx.store)
        idxpath = joinpath(ctx.store, idx)
        success, out, err = readcmd_with_index(ctx, cmd, idxpath)
        if success
            for (t,s) in out
                parts = split(s, ':'; limit=3)
                push!(results, (file=String(parts[1]), line=parse(Int,parts[2]), text=String(parts[3])))
            end
        end
    end
    results
end

end # module
