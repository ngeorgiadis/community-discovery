using Graphs, MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics
using MD5

include("config.jl")

DEBUG = true

function load_graph_data_simple(file)
    G = MetaGraph(1712433)
    # "N:/sources/01_datalab/2022/data/AMiner-Coauthor.txt"
    open(file) do f
        for ln in eachline(f)
            p = split(ln, "\t")
            if length(p) == 3
                index1 = parse(Int64, SubString(p[1], 2))
                index2 = parse(Int64, p[2])
                add_edge!(G, index1, index2)
            end
        end
    end
    return G
end

function load_graph_data_multigraph(file)
    G = MetaGraph(1712433)
    # "N:/sources/01_datalab/2022/data/AMiner-Coauthor.txt"
    open(file) do f
        for ln in eachline(f)
            p = split(ln, "\t")
            if length(p) == 3
                index1 = parse(Int64, SubString(p[1], 2))
                index2 = parse(Int64, p[2])
                add_edge!(G, index1, index2)
            end
        end
    end
    return G
end

function read_dom(file)

    max_degree = 0
    max_dom = 0

    dom_dict = Dict{Int64,Int64}()
    dom_array = []

    # "N:/sources/01_datalab/2022/data/auth_dom_scores.txt"
    open(file) do f
        for ln in eachline(f)
            p = split(ln, "\t")
            if length(p) == 2
                id = parse(Int64, p[1])
                dom = parse(Int64, p[2])
                # d = degree(G, id)

                # if d > max_degree
                #     max_degree = d
                # end

                if dom > max_dom
                    max_dom = dom
                end

                dom_dict[id] = dom
                push!(dom_array, Dict{Symbol,Any}(:id => id, :dom => dom))
            end
        end
    end

    sort!(dom_array, by=x -> (x[:dom], x[:id]), rev=true)

    return dom_dict, dom_array, max_dom
end

function get_graph_stats(attr, max_dom, max_core_number)

    stats = Dict{String,Any}()

    N = length(attr)
    square_sum = 0

    for n in attr
        square_sum += (n[:dom] - max_dom)^2
    end

    ratio_max_core = max_core_number / N
    stddev = sqrt(square_sum / N)

    stats["number_of_nodes"] = N
    stats["ratio_max_k_core"] = ratio_max_core
    stats["max_k_core"] = max_core_number
    stats["max_stddev"] = stddev
    stats["e2"] = (max_core_number * ratio_max_core) / stddev
    stats["e4"] = (max_core_number + ratio_max_core) / stddev

    return stats
end

function find_communities(g, dsa, top, hop, check_points)
    results = []
    visited = Dict{Int64,Bool}()
    i = 1
    max_dom = dsa[1][:dom]

    check1 = Base.time()
    checpoint_times = []
    cpi = 1

    for (idx, n) in enumerate(dsa[1:top])

        if (idx >= check_points[cpi])
            p1 = Base.time() - check1
            push!(checpoint_times, p1)
            println("")
            println("checkpoint: $(sum(checpoint_times)), top-$(check_points[cpi]) ( $(length(results)) ), $(hop)")
            cpi += 1
            check1 = Base.time()
        end

        if haskey(visited, n[:id])
            continue
        end

        e1 = egonet(g, n[:id], hop)

        v1 = filter(
            v ->
                get_prop(e1, v, :dom) > 0 && !haskey(visited, get_prop(e1, v, :id)),
            vertices(e1),
        )
        e2 = e1[v1]
        if (!is_connected(e2))
            init_idx = findfirst(v -> get_prop(e2, v, :id) == n[:id], vertices(e2))
            e2 = egonet(e2, init_idx, hop)
        end

        if nv(e2) == 0
            continue
        end
        corenum = core_number(e2)
        k = maximum(corenum)
        max_k_core = findall(x -> x >= k, corenum)
        k1, _ = induced_subgraph(e2, max_k_core)
        k2 = map(v -> props(k1, v), vertices(k1))

        for v in k2
            visited[v[:id]] = true
        end

        # get graph stats
        stats = get_graph_stats(k2, max_dom, k)
        stats["init"] = n[:id]
        stats["original_index"] = idx

        if DEBUG
            stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
            stats["density"] = density(k1)
            stats["avg_degree"] = 2 * ne(k1) / nv(k1)
        end

        push!(results, stats)

        if i % 1000 == 0
            print(".")
        end
        i += 1
    end

    return results
end

function main()
    # g = @time load_graph_data_simple()
    # didx, dsa, max_dom = @time read_dom()
    # @time find_communities(g, dsa, didx, 1000, 1)

    # linux path
    graph_file = "/mnt/n/sources/01_datalab/2022/data/AMiner-Coauthor.txt"

    # dom_files = [
    #     "/mnt/n/sources/01_datalab/2022/data/auth_dom_scores_2d.txt",
    #     "/mnt/n/sources/01_datalab/2022/data/auth_dom_scores_3d.txt",
    #     "/mnt/n/sources/01_datalab/2022/data/auth_dom_scores.txt"
    # ]

    # windows path
    # graph_file = "N:/sources/01_datalab/2022/data/AMiner-Coauthor.txt"
    # dom_file = "N:/sources/01_datalab/2022/data/auth_dom_scores.txt"

    println("graph_file: $(graph_file)")
    println("dom_file: $(dom_file)")
    println("hop: $(hop)")
    println("")

    g = @time load_graph_data_multigraph(graph_file)
    didx, dsa, max_dom = @time read_dom(dom_file)

    for (k, v) in didx
        set_prop!(g, k, :id, k)
        set_prop!(g, k, :dom, v)
    end

    # all = 581740
    check_points = [5000, 10000, 50000, 100000, 581740]
    results = @time find_communities(g, dsa, all, hop, check_points)

    println("total communities: $(length(results))")
    println("")

    #
    # SAVING RESULTS
    #

    df = DataFrame(init=Int64[],
        original_index=Int64[],
        number_of_nodes=Float64[],
        ratio_max_k_core=Float64[],
        max_k_core=Float64[],
        max_stddev=Float64[],
        e2=Float64[],
        e4=Float64[])

    if DEBUG
        df = DataFrame(init=Int64[],
            original_index=Int64[],
            number_of_nodes=Float64[],
            ratio_max_k_core=Float64[],
            max_k_core=Float64[],
            max_stddev=Float64[],
            e2=Float64[],
            e4=Float64[],
            avg_clustering=Float64[],
            density=Float64[],
            avg_degree=Float64[])
    end

    for r in results
        push!(df, r)
    end

    filter!(:number_of_nodes => x -> x > 1, df)

    #
    # TRANSFORM
    #

    transform!(df, :max_stddev => (v -> 100000 ./ v) => :std1)
    transform!(df, :std1 => (v -> v ./ maximum(v)) => :norm)
    transform!(
        df,
        [:norm, :ratio_max_k_core] => ((v1, v2) -> (v1 .* 0.75) + (v2 .* 0.25)) => :w1,
    )
    transform!(df, [:norm] => ((v1) -> floor.((v1 .* 500))) => :qnorm)

    if DEBUG
        transform!(df, [:norm, :avg_degree] => ((v1, v2) -> v1 .* v2) => :ad2)
    end

    sort!(df, [:qnorm, :ratio_max_k_core], rev=true)

    #
    # SAVE
    #
    open("results-$(all)-$(hop).csv", "w") do output
        CSV.write(output, df, delim=";")
    end

    # try free up memory
    g = nothing
    didx = nothing
    dsa = nothing
    results = nothing
    df = nothing

    GC.gc()
end

main()

# graph_file = "n:/sources/01_datalab/2022/data/AMiner-Coauthor.txt"
# dom_file = "n:/sources/01_datalab/2022/data/auth_dom_scores.txt"
# println("graph_file: $(graph_file)")
# println("dom_file: $(dom_file)")
# println("hop: $(hop)")
# println("")
# g = @time load_graph_data_multigraph(graph_file)
# didx, dsa, max_dom = @time read_dom(dom_file)
# for (k, v) in didx
#     set_prop!(g, k, :id, k)
#     set_prop!(g, k, :dom, v)
# end

# all = 5000
# check_points = [5000, 10000, 50000, 100000, 581740]
# results = @time find_communities(g, dsa, all, hop, check_points)

# df = DataFrame(init=Int64[],
#     original_index=Int64[],
#     number_of_nodes=Float64[],
#     ratio_max_k_core=Float64[],
#     max_k_core=Float64[],
#     max_stddev=Float64[],
#     e2=Float64[],
#     e4=Float64[])

# for r in results
#     push!(df, r)

# end
# transform!(df, :max_stddev => (v -> 100000 ./ v) => :std1)
# transform!(df, :std1 => (v -> v ./ maximum(v)) => :norm)
# transform!(
#     df,
#     [:norm, :ratio_max_k_core] => ((v1, v2) -> (v1 .* 0.75) + (v2 .* 0.25)) => :w1,
# )
# transform!(df, [:norm] => ((v1) -> floor.((v1 .* 500))) => :qnorm)
# sort!(df, [:qnorm, :ratio_max_k_core], rev=true)

# visited = Dict{Int64,Bool}()
# for n in dsa[1:293]
#     if haskey(visited, n[:id])
#         continue
#     end

#     e1 = egonet(g, n[:id], hop)
#     e2, _ = induced_subgraph(
#         e1,
#         filter(
#             v ->
#                 get_prop(e1, v, :dom) > 0 && !haskey(visited, get_prop(e1, v, :id)),
#             vertices(e1),
#         ),
#     )
#     if nv(e2) == 0
#         continue
#     end

#     corenum = core_number(e2)
#     k = maximum(corenum)
#     max_k_core = findall(x -> x >= k, corenum)
#     k1, _ = induced_subgraph(e2, max_k_core)
#     k2 = map(v -> props(k1, v), vertices(k1))

#     for v in k2
#         visited[v[:id]] = true
#     end

# end

# test = egonet(g, 1503903, 2)

# v = vertices(test)
# findfirst(v -> get_prop(test, v, :id) == 397845, v)
# e2, _ = induced_subgraph(
#     test,
#     filter(
#         v ->
#             get_prop(test, v, :dom) > 0 && !haskey(visited, get_prop(test, v, :id)),
#         vertices(test),
#     ),
# )


# init_idx = findfirst(v -> get_prop(e2, v, :id) == 1503903, vertices(e2))

# corenum = core_number(e2)
# k = maximum(corenum)
# max_k_core = findall(x -> x >= k, corenum)
# k1, _ = induced_subgraph(e2, max_k_core)
# k2 = map(v -> props(k1, v), vertices(k1))

# @time a_star(test, 1, 3999)
# fi = filter(v -> get_prop(test, v, :id) == 228335, vertices(test))

# findfirst(v -> v[:id] == 112976, dsa)


# h1, stats = find_community_non_overlapping(g, dsa, 1503903, 2, dsa[1][:dom], true)


# for v in vertices(test)
#     println(v)
#     if get_prop(test, v, :dom) <= 0 || haskey(visited, get_prop(test, v, :id))
#         rem_vertex!(test, v)
#     end
# end