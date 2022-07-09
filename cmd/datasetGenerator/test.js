const fs = require("fs");

let a = fs
  .readFileSync("domination.txt")
  .toString()
  .split("\n")
  .map((x) => {
    if (x.startsWith("id")) {
      return;
    }

    let p = x.split("\t");
    if (p.length == 2) {
      return {
        id: p[0],
        score: parseInt(p[1]),
      };
    }
  })
  .filter((x) => x)
  .sort((a, b) => {
    return b.score - a.score;
  });

console.log(a);
