using Graphs, MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics
using MD5

DEBUG = true

function load_graph_data_simple(file)
    G = MetaGraph(1712433)

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

    open(file) do f
        for ln in eachline(f)
            p = split(ln, "\t")
            if length(p) == 2
                id = parse(Int64, p[1])
                dom = parse(Int64, p[2])

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