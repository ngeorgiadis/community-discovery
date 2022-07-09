using Graphs
using MetaGraphs
using Dates
using DataFrames
using CSV
using Statistics

include("time.jl")

function main()

    println("starting...")

    dom_scores = [
        ("../data/auth_dom_scores_2d.txt", "2d"),
        ("../data/auth_dom_scores_3d.txt", "3d"),
        ("../data/auth_dom_scores.txt", "4d"),
    ]

    for dom_score_file in dom_scores

        g, dsa = load_graph_with_data(dom_score_file[1])

        # stats = get_communities(g, dsa, 1, 5000)
        # stats = sort(collect(stats), by = x -> x[2]["e4"], rev = true)
        # println(stats)

        #
        # this block is only for testing
        #
        # stats, communities = get_communities(g, dsa, 2, 1000)
        # transform!(stats, :max_stddev => (v -> 100000 ./ v) => :std1)
        # transform!(stats, :std1 => (v -> v ./ maximum(v)) => :norm)
        # transform!(
        #     stats,
        #     [:norm, :ratio_max_k_core] => ((v1, v2) -> (v1 .* 0.75) + (v2 .* 0.25)) => :w1,
        # )

        # transform!(
        #     stats,
        #     [:norm, :ratio_max_k_core] => ((v1, v2) -> (v1 .* 0.5) + (v2 .* 0.5)) => :w2,
        # )

        # transform!(
        #     stats,
        #     [:norm, :ratio_max_k_core] => ((v1, v2) -> (v1 .* 0.25) + (v2 .* 0.75)) => :w3,
        # )

        # sort!(stats, [:w1], rev = true)
        # open("test_2d_h2_1k.csv", "w") do output
        #     CSV.write(output, stats)
        # end

        #
        # end test block
        #

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

                sort(stats, [:w1], rev = true)
                # stats = sort(collect(stats), by = x -> x[2]["e4"], rev = true)
                open("out_new_$(t)_h$(h)_$(dom_score_file[2]).csv", "w") do output
                    CSV.write(output, stats)
                end
            end
        end
    end
end


main()
