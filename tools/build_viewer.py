import json
t = json.load(open("runs/replay_v3.json"))
open("web/trace.js", "w").write("window.TRACE = " + json.dumps(t, ensure_ascii=False) + ";\n")
print("days", len(t["days"]))
