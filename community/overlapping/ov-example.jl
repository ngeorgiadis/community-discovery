using Graphs
using MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics
using MD5

# this file (config.jl) includes runtime settings
# must be configured before run the script
include("runtime-config-example.jl")
include("../common.jl")

function load_graph_data_multigraph_example(file)
    G = MetaGraph(23)

    open(file) do f
        for ln in eachline(f)
            p = split(ln, "\t")
            if length(p) == 2
                index1 = parse(Int64, p[1])
                index2 = parse(Int64, p[2])
                add_edge!(G, index1, index2)
            end
        end
    end
    return G
end

function find_communities_overlapping(g, dsa, top, hop, check_points)

    results = []

    max_dom = dsa[1][:dom]

    check1 = Base.time()
    checpoint_times = []
    cpi = 1
    egotime = 0
    kcoretime = 0

    for (idx, n) in enumerate(dsa[1:top])

        if (idx > check_points[cpi])
            p1 = Base.time() - check1
            push!(checpoint_times, p1)
            println("")
            println("checkpoint: $(sum(checpoint_times)), top-$(check_points[cpi]), egonet time: $(egotime), k-core time:$(kcoretime) ( $(length(results)) ), $(hop)")
            cpi += 1
            check1 = Base.time()
        end

        t0 = Base.time()

        # find egonet
        e1 = egonet(g, n[:id], hop)
        # end 

        et = Base.time() - t0
        egotime = egotime + et

        e2 = e1
        if nv(e2) <= 1
            continue
        end

        t0 = Base.time()
        # find max k-core
        corenum = core_number(e2)
        k = maximum(corenum)
        max_k_core = findall(x -> x >= k, corenum)
        k1, _ = induced_subgraph(e2, max_k_core)
        # end

        kt = Base.time() - t0
        kcoretime = kcoretime + kt

        k2 = map(v -> props(k1, v), vertices(k1))

        # get graph stats
        stats = get_graph_stats(k2, max_dom, k)
        stats["init"] = n[:id]
        stats["original_index"] = idx
        stats["egotime"] = et
        stats["kcoretime"] = kt

        if DEBUG
            stats["avg_clustering"] = mean(local_clustering_coefficient(k1))
            stats["density"] = density(k1)
            stats["avg_degree"] = 2 * ne(k1) / nv(k1)

            nodes = ""
            for n in k2
                nodes = nodes * "$(n[:id]), "
            end
            stats["nodes"] = nodes
        end

        push!(results, stats)

        if idx % 1000 == 0
            print(".")
        end

    end

    return results, egotime, kcoretime
end

println("graph_file: $(graph_file)")
println("dom_file: $(dom_file)")
println("hop: $(hop)")
println("")

g = @time load_graph_data_multigraph_example(graph_file)
didx, dsa, max_dom = @time read_dom(dom_file)

for (k, v) in didx
    set_prop!(g, k, :id, k)
    set_prop!(g, k, :dom, v)
end

didx = nothing
GC.gc()

# variable `all` is defined
# in runtime-config.jl

check_points = [5000, 10000, 50000, 100000, 500000]
results, egotime, kcoretime = @time find_communities_overlapping(g, dsa, all, hop, check_points)

println("total communities: $(length(results)), egonet time: $(egotime), k-core time: $(kcoretime)")
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
    e4=Float64[],
    egotime=Float64[],
    kcoretime=Float64[]
)

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
        avg_degree=Float64[],
        egotime=Float64[],
        kcoretime=Float64[],
        nodes=String[]
    )
end

for r in results
    push!(df, r)
end

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
stmp = Dates.format(now(), "yyyymmdd-HHMMSS")
suffix = ""
if DEBUG
    suffix = "_DEBUG"
end

open("results-$(all)-$(hop)-$(stmp)$(suffix).csv", "w") do output
    CSV.write(output, df, delim=";")
end


# try free up memory
g = nothing
didx = nothing
dsa = nothing
results = nothing

GC.gc()
