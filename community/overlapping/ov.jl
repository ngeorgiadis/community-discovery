using Graphs
using MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics
using MD5

include("config.jl")


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

    didx = nothing
    GC.gc()

    # all = 581740
    check_points = [5000, 10000, 50000, 100000, 581740]
    results = @time find_communities_overlapping(g, dsa, all, hop, check_points)

    println("total communities: $(length(results))")
    println("")


    # try free up memory
    g = nothing
    didx = nothing
    dsa = nothing
    results = nothing

    GC.gc()
end

main()

