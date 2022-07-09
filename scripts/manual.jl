using Graphs
using MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics

include("time.jl")
include("config.jl")

function main()

    println("starting...")

    for dom_score_file in dom_scores

        g, dsa = load_graph_with_data(dom_score_file[1])





        top = [5000, 10000, 50000, 100000, 581740]
        hops = [1, 2, 3]
        for h in hops
            for t in top
                stats, communities = get_communities(g, dsa, h, t)

                transform!(stats, :max_stddev => (v -> 100000 ./ v) => :std1)
                transform!(stats, :std1 => (v -> v ./ maximum(v)) => :norm)
                transform!(
                    stats,
                    [:norm, :ratio_max_k_core] =>
                        ((v1, v2) -> (v1 .* 0.75) + (v2 .* 0.25)) => :w1,
                )

                sort(stats, [:w1], rev=true)
                # stats = sort(collect(stats), by = x -> x[2]["e4"], rev = true)
                open("out_$(t)_h$(h)_$(dom_score_file[2]).csv", "w") do output
                    CSV.write(output, stats)
                end
            end
        end
    end
end

##
# manual code
#

# dom_scores = [
#     ("../data/auth_dom_scores_2d.txt", "2d"),
#     ("../data/auth_dom_scores_3d.txt", "3d"),
#     ("../data/auth_dom_scores.txt", "4d"),
# ]

g, dsa, max_dom, max_degree = load_graph_with_data(dom_scores[3][1])


#
# normalize
#
map(x -> (x[:dom_norm] = x[:dom] / max_dom; return x), dsa)
map(x -> (x[:degree_norm] = x[:degree] / max_degree; return x), dsa)

#
# SORT dsa by new criteria
#

#
# In non-overlapping how we sort this array affects the 
# output of the communities, because once a node appears
# in a community it cannot be part of another
#

# sort!(dsa, by = x-> x[:dom_norm] + x[:degree_norm], rev=true)
sort!(dsa, by=x -> x[:dom_norm], rev=true)

#
# GET COMMUNITES
#
stats, communities, diff = get_communities(g, dsa, 1, 10000, max_dom)
# stats, communities, diff = get_communities(g, dsa, 1, 10000, max_dom)

#
# TRANSFORM
#
transform!(stats, :max_stddev => (v -> 100000 ./ v) => :std1)
transform!(stats, :std1 => (v -> v ./ maximum(v)) => :norm)
transform!(
    stats,
    [:norm, :ratio_max_k_core] => ((v1, v2) -> (v1 .* 0.75) + (v2 .* 0.25)) => :w1,
)
transform!(stats, [:norm] => ((v1) -> floor.((v1 .* 500))) => :qnorm)
transform!(stats, :avg_degree => (v -> v ./ maximum(v)) => :avg_degree_norm)
transform!(stats, [:norm, :avg_degree_norm] => ((v1, v2) -> v1 .+ v2) => :e7)
transform!(stats, [:norm, :avg_degree_norm] => ((v1, v2) -> v1 .* v2) => :e8)


#
# TAKES TOO LONG. DO NOT USE
#
# transform(stats, [:init] => ( (v) -> broadcast( x-> findfirst( y->y[:id]===x, dsa),v)) => :dsa)
#
#

# sort(stats, [:w1], rev = true)
filter!(:number_of_nodes => x -> x > 1, stats)
sort!(stats, [:qnorm, :ratio_max_k_core], rev=true)
# stats = sort(collect(stats), by = x -> x[2]["e4"], rev = true)
open("lefteris-non-overlap-hash-100k-diff.csv", "w") do output
    CSV.write(output, stats, delim=";")
end



# broadcast(x-> findfirst( y -> y == x, i)  , d)
df = DataFrame(diff, :auto)
open("diff2-non-ov-lefteris.csv", "w") do output
    CSV.write(output, df, delim=";")
end
