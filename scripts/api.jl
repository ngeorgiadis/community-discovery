using Genie, Genie.Router, Genie.Requests
using Genie.Renderer, Genie.Renderer.Html, Genie.Renderer.Json

macro wait()
    :(wait(Condition()))
end

include("./time.jl")
include("./logoutput.jl")
include("./config.jl")


# g_2d, dom_score_array_2d = load_graph_with_data(dom_scores[1][1])
# g_3d, dom_score_array_3d = load_graph_with_data(dom_scores[2][1])
# g_4d, dom_score_array_4d = load_graph_with_data(dom_scores[3][1])

g, dom_score_array, max_dom = load_graph_with_data(dom_scores[3][1])

route("/api/comm/:community_num/:hop") do
    hop = parse(Int64, payload(:hop))
    num = parse(Int64, payload(:commulity_num))
    (stats, comm) = get_community_overlap(g, dom_score_array, hop, num)

    json(
        Dict("stats" => stats, "community" => comm)
    )
end

route("/api/comm/:init/:hop") do
    hop = parse(Int64, payload(:hop))
    init = parse(Int64, payload(:init))
    (stats, comm) = get_community_overlap_by_init(g, dom_score_array, hop, init)
    json(
        Dict("stats" => stats, "community" => comm)
    )
end

route("/api/comm/non/:init/:hop") do
    hop = parse(Int64, payload(:hop))
    init = parse(Int64, payload(:init))

    (stats, comm) = get_community_non(g, dom_score_array, hop, init, max_dom)

    json(
        Dict("stats" => stats, "community" => comm)
    )
end

# route("/api/comm/non/:init/:hop") do
#     hop = parse(Int64, payload(:hop))
#     init = parse(Int64, payload(:init))

#     serve_static_file("./communities/$(hop)/$(init).json")
# end

up(9090, async=true)

@wait
