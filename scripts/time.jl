using Graphs
using MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics
using MD5

import Base

DEBUG = true

function load_graph_with_data(dom_score_file="../data/auth_dom_scores.txt")

    t1 = Base.time()
    print("creating graph... ")
    t0 = Base.time()
    G = MetaGraph(1712433)
    println(Base.time() - t0)

    print("adding edges... ")
    t0 = Base.time()
    open("../data/AMiner-Coauthor.txt") do f
        for ln in eachline(f)
            p = split(ln, "\t")
            if length(p) == 3
                index1 = parse(Int64, SubString(p[1], 2))
                index2 = parse(Int64, p[2])
                add_edge!(G, index1, index2)
            end
        end
    end
    println(Base.time() - t0)

    print("adding metadata (author stats)... ")
    t0 = Base.time()
    open("../data/AMiner-Author.txt") do file
        recs = 0
        id = 0
        name = ""
        pc = 0
        cn = 0
        hi = 0
        pidx = 0.0
        for ln in eachline(file)
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
                set_props!(
                    G,
                    id,
                    Dict(
                        :id => id,
                        :name => name,
                        :pc => pc,
                        :cn => cn,
                        :hi => hi,
                        :pi => pidx,
                    ),
                )
                recs += 1
            end
        end
    end
    println(Base.time() - t0)

    print("adding metadata (domination scores)... ")
    t0 = Base.time()

    dom_array = []
    max_dom = 0
    max_degree = 0
    open(dom_score_file) do file
        for ln in eachline(file)
            p = split(ln, "\t")
            if length(p) == 2
                id = parse(Int64, p[1])
                dom = parse(Int64, p[2])
                d = degree(G, id)
                set_prop!(G, id, :dom, dom)

                if d > max_degree
                    max_degree = d
                end

                if dom > max_dom
                    max_dom = dom
                end

                push!(dom_array, Dict{Symbol,Any}(:id => id, :dom => dom, :degree => d))
            end
        end
    end
    println(Base.time() - t0)

    print("sorting domination score array... ")
    t0 = Base.time()
    sort!(dom_array, by=x -> x[:dom], rev=true)
    println(Base.time() - t0)

    println("done! in ", Base.time() - t1)
    return G, dom_array, max_dom, max_degree
end

function get_graph_stats(g, attr, max_dom, max_core_number)
    s = Dict{String,Any}()
    N = length(attr)
    square_sum = 0

    for n in attr
        square_sum += (n[:dom] - max_dom)^2
    end

    ratio_max_core = max_core_number / N
    stddev = sqrt(square_sum / N)

    s["number_of_nodes"] = N
    s["ratio_max_k_core"] = ratio_max_core
    s["max_k_core"] = max_core_number

    s["max_stddev"] = stddev
    s["e2"] = (max_core_number * ratio_max_core) / stddev
    s["e4"] = (max_core_number + ratio_max_core) / stddev

    return s
end

"""
    get_communities

    Gets the top communitites by appling the `NON-OVERLAPPING` algorithm

"""
function get_communities(g, dsa, hop, top, max_dom)

    println("getting communities with params: $(hop)hop, top-$(top)")
    t0 = Base.time()
    i = 1
    visited = Dict{Int64,Bool}()
    exists = Dict{String,Bool}()
    results = DataFrame(
        init=Int64[],
        original_index=Int64[],
        number_of_nodes=Float64[],
        ratio_max_k_core=Float64[],
        max_k_core=Float64[],
        max_stddev=Float64[],
        e2=Float64[],
        e4=Float64[],
        density=Float64[],
        avg_degree=Float64[],
        avg_clustering=Float64[],
        hash=String[],
    )
    # max_dom = dsa[1][:dom]

    communities = Dict{Int64,Any}()

    latest = []
    diff_array = nothing

    for n in dsa[1:top]

        if haskey(visited, n[:id])
            continue
        end

        # find first the egonet of the node 
        # with distance $hop
        e1 = egonet(g, n[:id], hop)

        # filter the node with domination score 0
        e2, _ = induced_subgraph(
            e1,
            filter(
                v ->
                    get_prop(e1, v, :dom) > 0 && !haskey(visited, get_prop(e1, v, :id)),
                vertices(e1),
            ),
        )

        if nv(e2) == 0
            continue
        end

        # find the k-core
        corenum = core_number(e2)
        k = maximum(corenum)
        max_k_core = findall(x -> x >= k, corenum)

        # get the subgraph
        k1, _ = induced_subgraph(e2, max_k_core)

        # get the array with the subgraph properties
        k2 = map(v -> props(k1, v), vertices(k1))

        for v in k2
            visited[v[:id]] = true
        end

        stats = get_graph_stats(k1, k2, max_dom, k)

        if DEBUG
            #compute md5
            s = sort(map(x -> x[:id], values(k1.vprops)))
            hash = bytes2hex(md5(join(s, "|")))
            stats["hash"] = hash

            if haskey(exists, hash)
                continue
            else
                exists[hash] = true
            end
        else
            stats["hash"] = ""
        end

        # add more stats here
        stats["init"] = n[:id]
        stats["original_index"] = i
        # stats["nodes"] = k2


        if DEBUG
            communities[n[:id]] = k1
            stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
            stats["density"] = density(k1)
            stats["avg_degree"] = 2 * ne(k1) / nv(k1)
        else
            stats["avg_clustering"] = -1
            stats["density"] = -1
            stats["avg_degree"] = -1
        end

        push!(results, stats)

        if i % 1000 == 0
            print(".")
        end
        i += 1


        # if nrow(results) > 50
        #     c50 = transform(results, :max_stddev => (v -> 100000 ./ v) => :std1)
        #     transform!(c50, :std1 => (v -> v ./ maximum(v)) => :norm)
        #     transform!(
        #         c50,
        #         [:norm, :ratio_max_k_core] =>
        #             ((v1, v2) -> (v1 .* 0.75) + (v2 .* 0.25)) => :w1,
        #     )
        #     transform!(c50, [:norm] => ((v1) -> floor.((v1 .* 500))) => :qnorm)
        #     sort!(c50, [:qnorm, :ratio_max_k_core], rev=true)

        #     if latest != c50[1:50, :init]
        #         latest = c50[1:50, :init]

        #         t = [nrow(results), latest...]

        #         if diff_array === nothing
        #             diff_array = vcat(t')
        #         else
        #             diff_array = vcat(diff_array, t')
        #         end
        #     end
        # end

    end
    println("$(i), completed in ", Base.time() - t0)


    return results, communities, diff_array
end


"""
    get_community_non
    used in api.jl
"""
function get_community_non(g, dsa, hop, init, max_dom)

    println("getting communities with params: $(hop)hop, init-$(init)")
    t0 = Base.time()
    i = 1

    visited = Dict{Int64,Bool}()
    exists = Dict{String,Bool}()

    k1 = nothing
    stats = nothing

    for n in dsa

        if haskey(visited, init)
            break
        end

        if haskey(visited, n[:id])
            continue
        end

        # find first the egonet of the node 
        # with distance $hop
        e1 = egonet(g, n[:id], hop)

        # filter the node with domination score 0
        e2, _ = induced_subgraph(
            e1,
            filter(
                v ->
                    get_prop(e1, v, :dom) > 0 && !haskey(visited, get_prop(e1, v, :id)),
                vertices(e1),
            ),
        )

        if nv(e2) == 0
            continue
        end

        # find the k-core
        corenum = core_number(e2)
        k = maximum(corenum)
        max_k_core = findall(x -> x >= k, corenum)

        # get the subgraph
        k1, _ = induced_subgraph(e2, max_k_core)

        # get the array with the subgraph properties
        k2 = map(v -> props(k1, v), vertices(k1))

        for v in k2
            visited[v[:id]] = true
        end

        # if this is the node we want 
        # calculate the stat and break
        if n[:id] == init
            stats = get_graph_stats(k1, k2, max_dom, k)

            if DEBUG
                #compute md5
                s = sort(map(x -> x[:id], values(k1.vprops)))
                hash = bytes2hex(md5(join(s, "|")))
                stats["hash"] = hash

                if haskey(exists, hash)
                    continue
                else
                    exists[hash] = true
                end
            else
                stats["hash"] = ""
            end

            # add more stats here
            stats["init"] = n[:id]
            stats["original_index"] = i
            # stats["nodes"] = k2

            if DEBUG
                stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
                stats["density"] = density(k1)
                stats["avg_degree"] = 2 * ne(k1) / nv(k1)
            else
                stats["avg_clustering"] = -1
                stats["density"] = -1
                stats["avg_degree"] = -1
            end

            break
        end

        if i % 1000 == 0
            print(".")
        end

        i += 1
    end
    println("$(i), completed in ", Base.time() - t0)


    return stats, k1
end

"""
    get_communities_overlap

    Get the top communitites by appling the `OVERLAPPING` algorithm

"""
function get_communities_overlap(g, dsa, hop, top, max_dom)

    println("getting communities with params: $(hop)hop, top-$(top)")
    t0 = Base.time()
    i = 1
    # visited = Dict{Int64,Bool}()
    exists = Dict{String,Bool}()

    results = DataFrame(
        init=Int64[],
        original_index=Int64[],
        number_of_nodes=Float64[],
        ratio_max_k_core=Float64[],
        max_k_core=Float64[],
        max_stddev=Float64[],
        e2=Float64[],
        e4=Float64[],
        density=Float64[],
        avg_degree=Float64[],
        avg_clustering=Float64[],
        hash=String[],
    )

    # max_dom = dsa[1][:dom]

    communities = Dict{Int64,Any}()


    latest = []
    diff_array = nothing

    for n in dsa[1:top]

        # if haskey(visited, n[:id])
        #     continue
        # end

        # find first the egonet of the node 
        # with distance $hop
        e1 = egonet(g, n[:id], hop)

        # filter the node with domination score 0
        e2, _ = induced_subgraph(e1, filter(v -> get_prop(e1, v, :dom) > 0, vertices(e1)))

        if nv(e2) == 0
            continue
        end

        # find the k-core
        corenum = core_number(e2)
        k = maximum(corenum)
        max_k_core = findall(x -> x >= k, corenum)

        # get the subgraph
        k1, _ = induced_subgraph(e2, max_k_core)

        # get the array with the subgraph properties
        k2 = map(v -> props(k1, v), vertices(k1))

        # for v in k2
        #     visited[v[:id]] = true
        # end

        stats = get_graph_stats(k1, k2, max_dom, k)

        if DEBUG
            #compute md5
            s = sort(map(x -> x[:id], values(k1.vprops)))
            hash = bytes2hex(md5(join(s, "|")))
            stats["hash"] = hash

            if haskey(exists, hash)
                continue
            else
                exists[hash] = true
            end
        end


        # add more stats here
        stats["init"] = n[:id]
        stats["original_index"] = i
        # stats["nodes"] = k2

        if DEBUG
            communities[n[:id]] = k1
            stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
            stats["density"] = density(k1)
            stats["avg_degree"] = 2 * ne(k1) / nv(k1)
        else
            stats["avg_clustering"] = -1
            stats["density"] = -1
            stats["avg_degree"] = -1
            stats["md5"] = ""

        end

        push!(results, stats)

        if i % 1000 == 0
            print(".")
        end
        i += 1

        # if nrow(results) > 50
        #     c50 = transform(results, :max_stddev => (v -> 100000 ./ v) => :std1)
        #     transform!(c50, :std1 => (v -> v ./ maximum(v)) => :norm)
        #     transform!(
        #         c50,
        #         [:norm, :ratio_max_k_core] =>
        #             ((v1, v2) -> (v1 .* 0.75) + (v2 .* 0.25)) => :w1,
        #     )
        #     sort!(c50, [:w1], rev = true)



        #     if latest != c50[1:50, :init]
        #         latest = c50[1:50, :init]

        #         t = [nrow(results), latest...]

        #         if diff_array === nothing
        #             diff_array = vcat(t')
        #         else
        #             diff_array = vcat(diff_array, t')
        #         end
        #     end
        # end

    end
    println("$(i), completed in ", Base.time() - t0)


    return results, communities, diff_array
end


"""
Gets one community with the `OVERLAPPING` algorithm
"""
function get_community_overlap(g, dsa, hop, num)

    println("getting communities with params: $(hop)hop, $(num)")
    t0 = Base.time()

    # visited = Dict{Int64,Bool}()
    max_dom = dsa[1][:dom]

    n = dsa[num]

    # find first the egonet of the node 
    # with distance $hop
    e1 = egonet(g, n[:id], hop)

    # filter the node with domination score 0
    e2, _ = induced_subgraph(e1, filter(v -> get_prop(e1, v, :dom) > 0, vertices(e1)))

    # find the k-core
    corenum = core_number(e2)
    k = maximum(corenum)
    max_k_core = findall(x -> x >= k, corenum)

    # get the subgraph
    k1, _ = induced_subgraph(e2, max_k_core)

    # get the array with the subgraph properties
    k2 = map(v -> props(k1, v), vertices(k1))

    stats = get_graph_stats(k1, k2, max_dom, k)
    # add more stats here
    stats["init"] = n[:id]
    # stats["nodes"] = k2

    if DEBUG
        stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
        stats["density"] = density(k1)
    end

    println("completed in ", Base.time() - t0)


    return stats, k1
end

function get_community_overlap_by_init(g, dsa, hop, init)

    println("getting communities with params: $(hop)hop, init:$(init)")
    t0 = Base.time()

    # visited = Dict{Int64,Bool}()
    max_dom = dsa[1][:dom]

    # find first the egonet of the node 
    # with distance $hop
    e1 = egonet(g, init, hop)

    # filter the node with domination score 0
    e2, _ = induced_subgraph(e1, filter(v -> get_prop(e1, v, :dom) > 0, vertices(e1)))

    # find the k-core
    corenum = core_number(e2)
    k = maximum(corenum)
    max_k_core = findall(x -> x >= k, corenum)

    # get the subgraph
    k1, _ = induced_subgraph(e2, max_k_core)

    # get the array with the subgraph properties
    k2 = map(v -> props(k1, v), vertices(k1))

    stats = get_graph_stats(k1, k2, max_dom, k)
    # add more stats here
    stats["init"] = init
    # stats["nodes"] = k2

    if DEBUG
        stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
        stats["density"] = density(k1)
    end

    println("completed in ", Base.time() - t0)


    return stats, k1
end