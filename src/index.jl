const HEADER_BYTES = convert(Vector{UInt8}, codeunits("csearch index 1\n"))
const TRAILER_BYTES = convert(Vector{UInt8}, codeunits("\ncsearch trailr\n"))
const POSTING_LIST_TRAILER_TRIGRAM = UInt8[0xff, 0xff, 0xff]
const POSTING_LIST_TRAILER_DELTA = UInt32[0]

"""
The trailer has the form:
- offset of path list [4]
- offset of name list [4]
- offset of posting lists [4]
- offset of name index [4]
- offset of posting list index [4]
- "\ncsearch trailr\n"
"""
mutable struct IndexTrailerOffsets
    path_list::UInt32
    name_list::UInt32
    posting_list::UInt32
    name_index::UInt32
    posting_list_index::UInt32

    function IndexTrailerOffsets()
        new(0,0,0,0,0)
    end
end

"""
A single posting entry.

The trigram gives the 3 byte trigram that this list describes. The delta list is a sequence of varint-encoded deltas between file
IDs, ending with a zero delta.  For example, the delta list [2,5,1,1,0] encodes the file ID list 1, 6, 7, 8.

The delta list [0] would encode the empty file ID list, but empty posting lists are usually not recorded at all.

The list of posting lists ends with an entry with trigram "\xff\xff\xff" and a delta list consisting a single zero.
"""
struct Posting
    trigram::Vector{UInt8}
    deltas::Vector{UInt32}
end

const Postings = Vector{Posting}

fileids(posting::Posting) = fileids(posting.deltas)
fileids(deltas::Vector{UInt32}) = resize!(convert(Vector{UInt32}, cumsum(deltas)), UInt32(length(deltas)-1)) .- UInt32(1)
function deltas(fileids::Vector{UInt32})
    ids = convert(Vector{Int32}, fileids)
    pushfirst!(ids, -1)
    push!(convert(Vector{UInt32}, diff(ids)), 0)
end
function prune!(posting::Posting, id_map::Dict{UInt32,UInt32})
    fids = fileids(posting)
    filter!(x->x in keys(id_map), fids)
    map!(x->id_map[x], fids, fids)
    empty!(posting.deltas)
    dlts = deltas(fids)
    append!(posting.deltas, dlts)
    posting
end

function prune!(postings::Postings, id_map::Dict{UInt32,UInt32})
    for posting in postings
        prune!(posting, id_map)
    end
    # remove empty postings, except the trailer
    filter!(postings) do posting
        (posting.trigram == POSTING_LIST_TRAILER_TRIGRAM) || (length(posting.deltas) > 1)
    end
    postings
end

"""The name index is a sequence of 4-byte big-endian values listing the byte offset in the name list where each name begins."""
struct NameIndex
    entries::Vector{UInt32}

    function NameIndex()
        new(UInt32[])
    end
end

"""
The posting list index is a sequence of index entries describing each successive posting list.

Index entries are only written for the non-empty posting lists, so finding the posting list for a specific trigram requires a
binary search over the posting list index.  In practice, the majority of the possible trigrams are never seen, so omitting the missing
ones represents a significant storage savings.    
"""
struct PostingIndexEntry
    trigram::Vector{UInt8}
    filecount::UInt32
    offset::UInt32
end

struct PostingIndex
    entries::Vector{PostingIndexEntry}

    function PostingIndex()
        new(PostingIndexEntry[])
    end
end

struct Strings
    entries::Vector{String}

    function Strings(entries::Vector{String}=String[])
        new(entries)
    end
end

"""
Stores an index datastructure, which is either read from an existing file or prepared in memory.

An index stored on disk has the format:
- "csearch index 1\n"
- list of paths
- list of names
- list of posting lists
- name index
- posting list index
- trailer

The list of paths is a sorted sequence of NUL-terminated file or directory names. The index covers
the file trees rooted at those paths. The list ends with an empty name ("\x00").

The list of names is a sorted sequence of NUL-terminated file names. The initial entry in the list
corresponds to file #0, the next to file #1, and so on.  The list ends with an empty name ("\x00").

The list of posting lists are a sequence of posting lists. The list of posting lists ends with an
entry with trigram "\xff\xff\xff" and a delta list consisting a single zero.

The indexes enable efficient random access to the lists.

The trailer holds offsets of the various sections of the index.
"""
mutable struct Index
    paths::Strings
    names::Strings
    postings::Postings
    nameindex::NameIndex
    postingindex::PostingIndex
    offsets::IndexTrailerOffsets

    function Index()
        new(Strings(), Strings(), Posting[], NameIndex(), PostingIndex(), IndexTrailerOffsets())
    end
end

function find_exact_paths(idx::Index, paths::AbstractVector{String})
    exact_paths = String[]
    for path in paths
        for entry in idx.paths.entries
            if startswith(entry, path)
                push!(exact_paths, entry)
            end
        end
    end
    exact_paths
end

function find_name_indices(idx::Index, names::AbstractVector{String})
    nameidxs_to_prune = UInt32[]
    for (nameidx,name) in enumerate(idx.names.entries)
        if name in names
            push!(nameidxs_to_prune, nameidx)
        end
    end
    nameidxs_to_prune
end

# -----------------------------------------------
# index manipulation methods
# -----------------------------------------------
"""
Removes the path and all its descendants from the index.
"""
prune_paths!(idx::Index, path::String) = prune_paths!(idx, [path])
prune_paths!(idx::Index, paths::AbstractVector{String}) = prune_exact_paths!(idx, find_exact_paths(idx, paths))
function prune_exact_paths!(idx::Index, paths::Vector{String})
    # do nothing if possible
    isempty(paths) && (return idx)

    filter!(path->!(path in paths), idx.paths.entries)

    # prune names list and create ordered list of names and name indices that were pruned
    nameidxs_to_prune = UInt32[]
    names_to_prune = String[]
    for path in paths
        for (nameidx,name) in enumerate(idx.names.entries)
            if startswith(name, path)
                push!(nameidxs_to_prune, UInt32(nameidx-1))
                push!(names_to_prune, name)
            end
        end
    end
    prune_files!(idx, names_to_prune, nameidxs_to_prune)
end

prune_files!(idx::Index, name_to_prune::String) = prune_files!(idx, [name_to_prune])
prune_files!(idx::Index, names_to_prune::AbstractVector{String}) = prune_files!(idx, names_to_prune, find_name_indices(idx, names_to_prune))
function prune_files!(idx::Index, names_to_prune::AbstractVector{String}, nameidxs_to_prune::Vector{UInt32})
    isempty(nameidxs_to_prune) && (return idx)
    initial_names_count = UInt32(length(idx.names.entries))
    filter!(name->!(name in names_to_prune), idx.names.entries)

    # create a map of old name index to new index
    offset = 0
    namemap = Dict{UInt32,UInt32}()
    for idx in UInt32(0):initial_names_count
        if idx in nameidxs_to_prune
            offset += 1
        else
            namemap[idx] = idx-offset
        end
    end

    prune!(idx.postings, namemap)
    recreate_indices!(idx)
end

function recreate_indices!(idx::Index)
    idx.offsets.path_list = length(HEADER_BYTES)
    idx.offsets.name_list = path_list_size(idx) + idx.offsets.path_list
    idx.offsets.posting_list = recreate_name_index!(idx) + idx.offsets.name_list
    idx.offsets.name_index = recreate_posting_index!(idx) + idx.offsets.posting_list
    idx.offsets.posting_list_index = idx.offsets.name_index + UInt32(4 * (length(idx.names.entries) + 1))
    idx
end

function path_list_size(idx::Index)
    offset = 0
    for path in idx.paths.entries
        offset += UInt32(sizeof(path) + 1)
    end
    offset += UInt32(1) # for the null byte list terminator
    offset
end

function recreate_name_index!(idx::Index)
    empty!(idx.nameindex.entries)
    offset = UInt32(0)
    for name in idx.names.entries
        push!(idx.nameindex.entries, offset)
        offset += UInt32(sizeof(name) + 1)
    end
    push!(idx.nameindex.entries, offset) # for the end marker that we do not store in idx.names
    offset += UInt32(1) # for the null byte list terminator
    offset
end

function recreate_posting_index!(idx::Index)
    empty!(idx.postingindex.entries)
    offset = UInt32(0)
    for posting in idx.postings
        push!(idx.postingindex.entries, PostingIndexEntry(posting.trigram, UInt32(length(posting.deltas)-1), offset))
        offset += UInt32(3)
        for delta in posting.deltas
            offset += UInt32(nbytes_varint_8(delta))
        end
    end
    offset
end

# -----------------------------------------------
# base data reader and writer methods
# -----------------------------------------------
read_big_endian_4(io) = ntoh(read(io, UInt32))
write_big_endian_4(io, v::UInt32) = write(io, hton(v))

function read_varint_8(io)
    res = zero(UInt64)
    n = 0
    byte = UInt8(0x80)
    while (byte & 0x80) != 0
        byte = read(io, UInt8)
        res |= convert(UInt64, byte & 0x7f) << (7*n)
        n += 1
    end
    @assert n > 0
    res
end

function write_varint_8(io::IO, x::UInt32)
    nw = 0
    cont = true
    while cont
        byte = x & 0x7f
        if (x >>>= 7) != 0
            byte |= 0x80
        else
            cont = false
        end
        nw += write(io, UInt8(byte))
    end
    nw
end

function nbytes_varint_8(x::UInt32)
    nw = 0
    cont = true
    while cont
        byte = x & 0x7f
        if (x >>>= 7) != 0
            byte |= 0x80
        else
            cont = false
        end
        nw += 1
    end
    nw
end

# -----------------------------------------------
# reader methods
# -----------------------------------------------
function read(io::IO, idx::Index)
    total_file_size = UInt32(filesize(io))

    # validate header
    header_bytes = read(io, length(HEADER_BYTES))
    (header_bytes != HEADER_BYTES) && error("Not a valid index file")

    # validate trailer
    seek(io, total_file_size - length(TRAILER_BYTES))
    trailer_bytes = read(io, length(TRAILER_BYTES))
    (trailer_bytes != TRAILER_BYTES) && error("Not a valid index file")

    # read trailer offsets
    seek(io, filesize(io) - length(TRAILER_BYTES) - 5*4)
    read(io, idx.offsets)

    idx.paths = read_strings(io, idx.offsets.path_list, idx.offsets.name_list - idx.offsets.path_list)
    idx.names = read_strings(io, idx.offsets.name_list, idx.offsets.posting_list - idx.offsets.name_list)
    idx.postings = read_postings(io, idx.offsets.posting_list, idx.offsets.name_index - idx.offsets.posting_list)
    idx.nameindex = read_name_index(io, idx.offsets.name_index, idx.offsets.posting_list_index - idx.offsets.name_index)
    idx.postingindex = read_posting_index(io, idx.offsets.posting_list_index, UInt32(total_file_size - length(TRAILER_BYTES) - 5*4 - idx.offsets.posting_list_index))

    idx
end

function read_strings(io, pos::UInt32, nbytes::UInt32)
    seek(io, pos)
    Strings(String.(split(String(read(io, nbytes)), '\0'; keepempty=false)))
end

function read_posting(io)
    trigram = read(io, 3)
    deltas = UInt32[]
    while !eof(io)
        delta = UInt32(read_varint_8(io))
        push!(deltas, delta)
        (delta == 0) && break
    end
    Posting(trigram, deltas)
end

function read_name_index(io, pos::UInt32, nbytes::UInt32)
    seek(io, pos)
    endpos = pos + nbytes
    nameindex = NameIndex()
    indices = nameindex.entries
    while !eof(io) && (position(io) < endpos)
        push!(indices, read_big_endian_4(io))
    end
    nameindex
end

function read_posting_index_entry(io)
    trigram = read(io, 3)
    filecount = read_big_endian_4(io)
    offset = read_big_endian_4(io)
    PostingIndexEntry(trigram, filecount, offset)
end

function read_posting_index(io, pos::UInt32, nbytes::UInt32)
    seek(io, pos)
    endpos = pos + nbytes
    postingindex = PostingIndex()
    indices = postingindex.entries
    while !eof(io) && (position(io) < endpos)
        push!(indices, read_posting_index_entry(io))
    end
    postingindex
end

function read_postings(io, pos::UInt32, nbytes::UInt32)
    seek(io, pos)
    endpos = pos + nbytes
    postings = Posting[]
    # need a position check also because in practice some index files may not contain the trailer bytes
    while !eof(io) && (position(io) < endpos)
        posting = read_posting(io)
        push!(postings, posting)
        (posting.trigram == POSTING_LIST_TRAILER_TRIGRAM) && (posting.deltas == POSTING_LIST_TRAILER_DELTA) && break
    end
    postings
end

function read(io::IO, offsets::IndexTrailerOffsets)
    offsets.path_list = read_big_endian_4(io)
    offsets.name_list = read_big_endian_4(io)
    offsets.posting_list = read_big_endian_4(io)
    offsets.name_index = read_big_endian_4(io)
    offsets.posting_list_index = read_big_endian_4(io)

    offsets
end

# -----------------------------------------------
# writer methods
# -----------------------------------------------
function write(io::IO, offsets::IndexTrailerOffsets)
    n = 0
    n += write_big_endian_4(io, offsets.path_list)
    n += write_big_endian_4(io, offsets.name_list)
    n += write_big_endian_4(io, offsets.posting_list)
    n += write_big_endian_4(io, offsets.name_index)
    n += write_big_endian_4(io, offsets.posting_list_index)
    n
end

function write(io::IO, strings::Strings)
    n = 0
    for str in strings.entries
        n += write(io, str)
        n += write(io, 0x00)
    end
    n += write(io, 0x00)
    n
end

function write(io::IO, posting::Posting)
    n = 0
    n += write(io, posting.trigram)
    for delta in posting.deltas
        n += write_varint_8(io, delta)
    end
    n
end

function write(io::IO, postings::Postings)
    n = 0
    for posting in postings
        n += write(io, posting)
    end
    n
end

function write(io::IO, nameindex::NameIndex)
    n = 0
    for entry in nameindex.entries
        n += write_big_endian_4(io, entry)
    end
    n
end

function write(io::IO, entry::PostingIndexEntry)
    n = 0
    n += write(io, entry.trigram)
    n += write_big_endian_4(io, entry.filecount)
    n += write_big_endian_4(io, entry.offset)
    n
end

function write(io::IO, postingindex::PostingIndex)
    n = 0
    for entry in postingindex.entries
        n += write(io, entry)
    end
    n
end

function write(io::IO, idx::Index)
    n = 0
    n += write(io, HEADER_BYTES)
    n += write(io, idx.paths)
    n += write(io, idx.names)
    n += write(io, idx.postings)
    n += write(io, idx.nameindex)
    n += write(io, idx.postingindex)
    n += write(io, idx.offsets)
    n += write(io, TRAILER_BYTES)
    n
end
