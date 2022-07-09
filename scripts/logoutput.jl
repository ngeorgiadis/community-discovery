using Dates


function create_log()
    now_string = Dates.format(now(), "yyyymmdd_HHMMSS")
    return open("log_$(now_string).log", "a")
end

function write_to_log(io, msg, inlude_timestamp = true)
    if inlude_timestamp
        now_stamp = Dates.format(now(), "yyyy-mm-dd HH:MM:SS.ssss")
        write(io, "[$(now_stamp)] $(msg)")
        print("[$(now_stamp)] $(msg)")
    else
        write(io, "$(msg)")
        print("$(msg)")
    end
    flush(io)
end

function writeln_to_log(io, msg, inlude_timestamp = true)
    if inlude_timestamp
        now_stamp = Dates.format(now(), "yyyy-mm-dd HH:MM:SS.ssss")
        write(io, "[$(now_stamp)] $(msg)\n")
        println("[$(now_stamp)] $(msg)")
    else
        write(io, "$(msg)\n")
        println("$(msg)")
    end
    flush(io)
end





