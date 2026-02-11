#!/usr/bin/env python3
"""Build skills/ directory from source for npx skills discovery."""

from pathlib import Path
import shutil
import re
import yaml

def build_skills():
    root = Path(__file__).parent.parent
    skills_out = root / 'skills'
    
    # Clean and recreate
    if skills_out.exists():
        shutil.rmtree(skills_out)
    skills_out.mkdir()
    
    # Find all source skills
    sources = []
    for p in root.rglob('SKILL.md'):
        s = str(p)
        if any(x in s for x in ['/skills/', '/.git/', '/.ruff_cache/']):
            continue
        txt = p.read_text(errors='ignore')
        m = re.match(r'^---\n(.*?)\n---\n', txt, re.S)
        if not m:
            continue
        fm = yaml.safe_load(m.group(1)) or {}
        name = fm.get('name')
        if not name:
            continue
        sources.append((name, p.parent))
    
    # Deduplicate
    seen = {}
    for name, dirp in sources:
        seen.setdefault(name, dirp)
    
    # Copy to skills/
    for name, src_dir in sorted(seen.items()):
        dst = skills_out / name
        dst.mkdir(parents=True, exist_ok=True)
        
        # Copy SKILL.md and siblings
        for child in src_dir.iterdir():
            if child.name.startswith('.git') or child.name == '.ruff_cache':
                continue
            target = dst / child.name
            if child.is_file():
                shutil.copy2(child, target)
            elif child.is_dir():
                if any(p.name == 'SKILL.md' for p in child.rglob('SKILL.md')):
                    continue
                shutil.copytree(child, target)
        
        # Copy shared references for data-science
        if 'data-science' in str(src_dir):
            shared_refs = root / 'data-science' / 'references'
            if shared_refs.exists():
                dst_refs = dst / 'references'
                dst_refs.mkdir(exist_ok=True)
                for ref_file in shared_refs.iterdir():
                    if ref_file.is_file():
                        shutil.copy2(ref_file, dst_refs / ref_file.name)
    
    # Create README
    readme = skills_out / 'README.md'
    lines = [
        '# Generated Skills Directory\n\n',
        'This directory is auto-generated from source. Do not edit manually.\n\n',
        f'## {len(seen)} Skills\n\n'
    ]
    for name in sorted(seen):
        lines.append(f'- `{name}`\n')
    readme.write_text(''.join(lines))
    
    print(f'Built {len(seen)} skills to skills/')

if __name__ == '__main__':
    build_skills()
