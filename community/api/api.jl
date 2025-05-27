using Genie, Genie.Router, Genie.Requests
using Genie.Renderer, Genie.Renderer.Html, Genie.Renderer.Json
using Graphs
using MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics
using MD5

include("../common.jl")

macro wait()
    :(wait(Condition()))
end

function find_communities_overlapping(g, dsa, top, hop, check_points)
    results = Array{Dict{String,Any}}(undef, top)

    max_dom = dsa[1][:dom]

    check1 = Base.time()
    checpoint_times = []
    cpi = 1

    for (idx, n) in enumerate(dsa[1:top])

        if (idx > check_points[cpi])
            p1 = Base.time() - check1
            push!(checpoint_times, p1)
            println("")
            println("checkpoint: $(sum(checpoint_times)), top-$(check_points[cpi]) ( $(length(results)) ), $(hop)")
            cpi += 1
            check1 = Base.time()
        end


        e1 = egonet(g, n[:id], hop)
        e2, _ = induced_subgraph(
            e1,
            filter(
                v ->
                    get_prop(e1, v, :dom) > 0,
                vertices(e1),
            ),
        )
        if nv(e2) == 0
            continue
        end
        corenum = core_number(e2)
        k = maximum(corenum)
        max_k_core = findall(x -> x >= k, corenum)
        k1, _ = induced_subgraph(e2, max_k_core)
        k2 = map(v -> props(k1, v), vertices(k1))

        # get graph stats
        stats = get_graph_stats(k2, max_dom, k)
        stats["init"] = n[:id]
        stats["original_index"] = idx
        results[idx] = stats

        if idx % 1000 == 0
            print(".")
        end

    end

    return results
end

function find_community_overlapping(g, init, hop, max_dom, get_max_core)

    e1 = egonet(g, init, hop)

    e2 = e1
    # e2, _ = induced_subgraph(
    #     e1,
    #     filter(
    #         v ->
    #             get_prop(e1, v, :dom) > 0,
    #         vertices(e1),
    #     ),
    # )

    if (get_max_core)
        corenum = core_number(e2)
        k = maximum(corenum)
        max_k_core = findall(x -> x >= k, corenum)
        k1, _ = induced_subgraph(e2, max_k_core)
        k2 = map(v -> props(k1, v), vertices(k1))

        # get graph stats
        stats = get_graph_stats(k2, max_dom, k)
        stats["init"] = init
        stats["original_index"] = nothing

        return k1, stats
    end

    # get graph stats
    stats = get_graph_stats(map(v -> props(e2, v), vertices(e2)), max_dom, 1)
    stats["init"] = init
    stats["original_index"] = nothing

    return e2, stats
end

function find_community_non_overlapping(g, dsa, init, hop, max_dom, get_max_core)

    println("getting communities with params: $(hop)hop, init-$(init)")
    t0 = Base.time()

    visited = Dict{Int64,Bool}()
    i = 1

    community = nothing
    stats = nothing

    for (idx, n) in enumerate(dsa)

        if haskey(visited, init)
            break
        end

        if haskey(visited, n[:id])
            continue
        end

        e1 = egonet(g, n[:id], hop)
        v1 = filter(
            v -> !haskey(visited, get_prop(e1, v, :id)),
            vertices(e1),
        )

        e2 = e1[v1]
        if nv(e2) <= 1
            continue
        end

        if (get_max_core)
            corenum = core_number(e2)
            k = maximum(corenum)
            max_k_core = findall(x -> x >= k, corenum)
            k1, _ = induced_subgraph(e2, max_k_core)
            k2 = map(v -> props(k1, v), vertices(k1))

            for v in k2
                visited[v[:id]] = true
            end
        else
            e3 = map(v -> props(e2, v), vertices(e2))
            for v in e3
                visited[v[:id]] = true
            end
        end

        if (get_max_core)
            # get graph stats
            stats = get_graph_stats(k2, max_dom, k)
            stats["init"] = n[:id]
            stats["original_index"] = idx

            if DEBUG
                stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
                stats["density"] = density(k1)
                stats["avg_degree"] = 2 * ne(k1) / nv(k1)
            end

            community = k1
        else
            stats = get_graph_stats(map(v -> props(e2, v), vertices(e2)), max_dom, 1)
            stats["init"] = n[:id]
            stats["original_index"] = i

            community = e2
        end

        if i % 1000 == 0
            print(".")
        end
        i += 1

        if n[:id] == init
            break
        end
    end
    println("$(i), completed in ", Base.time() - t0)

    return community, stats
end

function read_attrs(file)
    println("adding metadata (author stats)... ")

    results = Dict()

    open(file) do f

        id = 0
        name = ""
        pc = 0
        cn = 0
        hi = 0
        pidx = 0.0
        for ln in eachline(f)
            if findfirst("#index ", ln) !== nothing
                id = parse(Int64, replace(ln, "#index " => ""))
            elseif findfirst("#n ", ln) !== nothing
                name = replace(ln, "#n " => "")
            elseif findfirst("#pc ", ln) !== nothing
                pc = parse(Int64, replace(ln, "#pc " => ""))
            elseif findfirst("#cn ", ln) !== nothing
                cn = parse(Int64, replace(ln, "#cn " => ""))
            elseif findfirst("#hi ", ln) !== nothing
                hi = parse(Int64, replace(ln, "#hi " => ""))
            elseif findfirst("#pi ", ln) !== nothing
                pidx = parse(Float64, replace(ln, "#pi " => ""))
            elseif ln == ""
                results[id] = Dict(
                    :id => id,
                    :name => name,
                    :pc => pc,
                    :cn => cn,
                    :hi => hi,
                    :pi => pidx,
                )
            end
        end
    end

    return results
end

graph_file = "N:/sources/01_datalab/2022/data/AMiner-Coauthor.txt"
attrs_file = "N:/sources/01_datalab/2022/data/AMiner-Author.txt"
dom_file = "N:/sources/01_datalab/2022/data/auth_dom_scores.txt"

g = @time load_graph_data_multigraph(graph_file)
attrs = @time read_attrs(attrs_file)
didx, dsa, max_dom = @time read_dom(dom_file)

for (id, attr) in attrs
    set_props!(g, id, attr)
end

for (k, v) in didx
    # set_prop!(g, k, :id, k)
    set_prop!(g, k, :dom, v)
end



didx = nothing
attrs = nothing
GC.gc()

route("/api/comm/over/:init/:hop/:max") do
    hop = parse(Int64, payload(:hop))
    init = parse(Int64, payload(:init))

    max = payload(:max) == "max"

    (comm, stats) = @time find_community_overlapping(g, init, hop, max_dom, max)

    json(
        Dict("stats" => stats, "community" => comm)
    )
end

route("/api/comm/non/:init/:hop/:max") do
    hop = parse(Int64, payload(:hop))
    init = parse(Int64, payload(:init))

    max = payload(:max) == "max"

    (comm, stats) = @time find_community_non_overlapping(g, dsa, init, hop, max_dom, max)

    json(
        Dict("stats" => stats, "community" => comm)
    )
end

up(9090, async=true)

@wait
