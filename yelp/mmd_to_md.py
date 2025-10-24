# wrap_mmd_to_md.py
import glob, os
for p in glob.glob("*.mmd"):
    with open(p, "r", encoding="utf-8") as f:
        txt = f.read().strip()
    md = f"```mermaid\n{txt}\n```\n"
    out = os.path.splitext(p)[0] + ".md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(md)
    print("Wrote", out)
