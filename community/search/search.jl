using Graphs, MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics
using MD5
using TOML
using GraphIO.GML
using GraphPlot

struct Config
    nodes_file::String
    edges_file::String
    query::Int64
    hop::Int8
end

function get_config()
    c = TOML.parsefile("community/search/config.toml")

    return Config(
        c["nodes_file"],
        c["edges_file"],
        c["query"],
        c["hop"]
    )
end

function find_communities(g, dsa, top, hop, check_points)
    results = []
    visited = Dict{Int64,Bool}()
    i = 1
    max_dom = dsa[1][:dom]

    check1 = Base.time()
    checpoint_times = []
    cpi = 1
    egotime = 0
    kcoretime = 0

    for (idx, n) in enumerate(dsa[1:top])

        if (idx >= check_points[cpi])
            p1 = Base.time() - check1
            push!(checpoint_times, p1)
            println("")
            println("checkpoint: $(sum(checpoint_times)), top-$(check_points[cpi]), egonet time: $(egotime), k-core time:$(kcoretime) ( $(length(results)) ), $(hop)")
            cpi += 1
            check1 = Base.time()
        end

        if haskey(visited, n[:id])
            continue
        end

        t0 = Base.time()

        # find egonet
        e1 = egonet(g, n[:id], hop)
        # end

        et = Base.time() - t0
        egotime = egotime + et

        v1 = filter(
            v -> !haskey(visited, get_prop(e1, v, :id)),
            vertices(e1),
        )

        e2 = e1[v1]
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

        for v in k2
            visited[v[:id]] = true
        end

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

        if i % 1000 == 0
            print(".")
        end
        i += 1
    end

    return results, egotime, kcoretime
end

function create_graph(file)
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

    for (i, v) in enumerate(vertices(G))
        set_prop!(G, v, :id, v)
    end

    return G
end

function a_dominates_b(a, b)

    if size(a, 1) != size(b, 1)
        error("vectors should be the same length")
    end

    at_least_one_better = false
    for i in axes(a, 1)
        if a[i] < b[i]
            return false
        end

        # it will come to this point 
        # if a[i] >= b[i]

        # mark if there is at least one element
        # a strictly better than b
        if a[i] > b[i]
            at_least_one_better = true
        end
    end
    return at_least_one_better
end

"""
    domination_score(v, m)

    v is a Vector of length N
    m is a Matrix of size M x N

    Gets the domination score of a vector compared to other vectors
    passed as a matrix. This implementation is just a double loop 
    which is done for testing purposes and in case of large matrix will
    take long time to complete.
"""
function domination_score(v, m)
    score = 0
    for i in axes(m, 1)
        # if v dominates current row inc score
        if a_dominates_b(v, m[i, 2:5])
            score = score + 1
        end
    end
    return score
end

function main()

    c = get_config()

    println("nodes file: $(c.nodes_file)")
    println("edges file: $(c.edges_file)")
    println("query: $(c.query)")
    println("hop: $(c.hop)")


    print("loading graph...")
    g = @time create_graph(c.edges_file)
    println("done!")

    print("loading nodes...")
    df = CSV.read(c.nodes_file, DataFrame)
    df[!, :pi] = trunc.(Int64, df[!, :pi])
    # node_attrs = Matrix(df[:, [:Id, :pc, :cn, :hi, :pi]])
    println("done!")

    print("searching egonet with $(c.hop + 1) hops...")
    egograph = @time egonet(g, c.query, c.hop + 1)
    println("done!")

    println("egograph contains $(nv(egograph)) vertices and $(ne(egograph)) edges.")


    print("saving graph to file...")
    savegraph("egograph.dot", egograph, MetaGraphs.DOTFormat())
    println("done!")

    m = map(x -> get_prop(egograph, x, :id), vertices(egograph))
    egograph_attrs = df[m, :]

    # create new column and set all values in the column to -1
    egograph_attrs[!, :dom] .= -1

    egograph_attrs_mat = Matrix(egograph_attrs[:, [:Id, :pc, :cn, :hi, :pi]])

    print("calculating domination scores...")

    for (i, row) in enumerate(eachrow(egograph_attrs))
        #id = row[:Id]
        #name = row[:Label]
        attrs = Vector(row[[:pc, :cn, :hi, :pi]])

        # attrs = Matrix(df[m, [:Id, :pc, :cn, :hi, :pi]])
        ds = domination_score(attrs, egograph_attrs_mat)
        egograph_attrs[i, :dom] = ds
        # println("$(i) -> $(id): $(name) ( $(attrs)) [ $(ds)]")
        if mod(i, 100) == 0
            print(".")
        end
    end

    println("done!")

    #
    # export the results
    #
    print("saving csv file...")
    stmp = Dates.format(now(), "yyyymmdd-HHMMSS")
    sort!(egograph_attrs, [:dom, :Id], rev=true)
    open("results-$(stmp).csv", "w") do output
        CSV.write(output, egograph_attrs, delim=";")
    end
    println("done!")


    # for (i, v) in enumerate(vertices(egograph))
    #     id = get_prop(egograph, v, :id)
    #     name = df[id, :Label]
    #     attrs = Vector(df[id, [:pc, :cn, :hi, :pi]])
    #     # attrs = Matrix(df[m, [:Id, :pc, :cn, :hi, :pi]])
    #     ds = domination_score(attrs, egograph_attrs_mat)

    #     println("$(v) -> $(id): $(name) ( $(attrs)) [ $(ds)]")
    # end



    # corenum = core_number(e1)
    # k = maximum(corenum)
    # max_k_core = findall(x -> x >= k, corenum)
    # k1, _ = induced_subgraph(e1, max_k_core)
end

main()