#!/usr/bin/env python3
"""
Convert a continuous wetten.overheid.nl crawl dump into a Markdown file
with structured formatting of Dutch law pages (title, metadata, TOC, articles).

Usage:
  python convert_wetten_crawled_to_markdown.py INPUT.txt OUTPUT.md
"""

import re
import sys


def parse_blocks(lines):
    start_re = re.compile(r'^--- Content from: (.+?) ---$')
    end_re = re.compile(r'^--- End of content from: .+? ---$')
    blocks = []
    current_url = None
    current_lines = []
    in_block = False
    for raw in lines:
        line = raw.rstrip('\r\n')
        m = start_re.match(line)
        if m:
            current_url = m.group(1)
            current_lines = []
            in_block = True
            continue
        if in_block and end_re.match(line):
            blocks.append((current_url, current_lines))
            in_block = False
            current_url = None
            current_lines = []
            continue
        if in_block:
            current_lines.append(line)
    return blocks


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} INPUT.txt OUTPUT.md", file=sys.stderr)
        sys.exit(1)
    infile, outfile = sys.argv[1], sys.argv[2]
    with open(infile, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    blocks = parse_blocks(lines)
    with open(outfile, 'w', encoding='utf-8', newline='\n') as out:
        count = 0
        for url, content in blocks:
            # only format law pages (skip non-BWBR pages)
            if '/BWBR' not in url:
                continue
            count += 1
            # assemble raw text and strip common interface noise substrings
            raw = ' '.join(content).strip()
            noise_patterns = [
                'Toon relaties in LiDO', 'Maak een permanente link', 'Toon wetstechnische informatie',
                'Geen andere versie om mee te vergelijken', 'Vergelijk met andere versie',
                'Vergelijken met andere versie', 'Druk de regeling af', 'Druk het regelingonderdeel af',
                'Sla de regeling op', 'Sla het regelingonderdeel op', 'Externe relaties', 'Linktool', '...'
            ]
            for pat in noise_patterns:
                raw = raw.replace(pat, '')
            raw = re.sub(r'\s{2,}', ' ', raw).strip()
            # law identifier from URL
            m_id = re.search(r'/([A-Z0-9]+)(?:/|$)', url)
            law_id = m_id.group(1) if m_id else url
            # law title from header: between 'Regeling - ' and ' - <law_id>'
            title = law_id
            title_re = re.compile(r'Regeling\s*-\s*(.*?)\s*-\s*' + re.escape(law_id))
            m_title = title_re.search(raw)
            if m_title:
                title = m_title.group(1).strip()
            # extract metadata
            meta_re = re.compile(
                r'Geraadpleegd op\s*([0-9]{2}-[0-9]{2}-[0-9]{4})\.\s*'
                r'Geldend van\s*([0-9]{2}-[0-9]{2}-[0-9]{4})\s*t/m\s*([0-9]{2}-[0-9]{2}-[0-9]{4}|heden)'
            )
            m_meta = meta_re.search(raw)
            geraadpleegd = geldend_van = geldend_tot = None
            if m_meta:
                geraadpleegd, geldend_van, geldend_tot = m_meta.groups()
            # extract table of contents block
            toc_raw = ''
            toc_re = re.compile(r'Inhoudsopgave\s+(.*?)\s*' + re.escape(title), re.DOTALL)
            m_toc = toc_re.search(raw)
            if m_toc:
                toc_raw = m_toc.group(1).strip()
            # extract intro (opschrift en aanhef)
            intro = ''
            intro_re = re.compile(r'Origineel opschrift en aanhef\s*(.*?)\s*Artikel\s+1', re.DOTALL)
            m_intro = intro_re.search(raw)
            if m_intro:
                intro = m_intro.group(1).strip()
            # extract articles
            articles = []
            art_re = re.compile(r'(Artikel\s+([\d.]+)\s*.*?)(?=(?:Artikel\s+[\d.]+)|Origineel slotformulier)', re.DOTALL)
            for m_art in art_re.finditer(raw):
                num = m_art.group(2)
                text = m_art.group(1).strip()
                articles.append((num, text))
            # extract closing (slotformulier)
            closing = ''
            closing_re = re.compile(r'Origineel slotformulier en ondertekening\s*(.*?)$', re.DOTALL)
            m_cl = closing_re.search(raw)
            if m_cl:
                closing = m_cl.group(1).strip()

            # write Markdown
            if count > 1:
                out.write('\n---\n\n')
            out.write(f'# {title}\n\n')
            out.write(f'*Document: [{law_id}]({url})*\n\n')
            if geraadpleegd and geldend_van and geldend_tot:
                out.write(f'> **Geraadpleegd op:** {geraadpleegd}  \n')
                out.write(f'> **Geldend van:** {geldend_van} t/m {geldend_tot}\n\n')
            if toc_raw:
                out.write('## Inhoudsopgave\n\n')
                # split into chapters, sections, and articles
                entries = re.split(r' (?=(?:Hoofdstuk|Afdeling|Artikel))', toc_raw)
                for entry in entries:
                    entry = entry.strip()
                    if not entry:
                        continue
                    if entry.startswith('Hoofdstuk'):
                        out.write(f'- {entry}\n')
                    elif entry.startswith('Afdeling'):
                        out.write(f'  - {entry}\n')
                    elif entry.startswith('Artikel'):
                        out.write(f'    - {entry}\n')
                    else:
                        out.write(f'- {entry}\n')
                out.write('\n')
            if intro:
                out.write('## Origineel opschrift en aanhef\n\n')
                out.write(intro + '\n\n')
            if articles:
                for num, art_text in articles:
                    out.write(f'### Artikel {num}\n\n')
                    # split article heading from numbered paragraphs (starting at paragraph 1)
                    m_para_start = re.search(r'\b1\s+', art_text)
                    if m_para_start:
                        header = art_text[:m_para_start.start()].strip()
                        paras_text = art_text[m_para_start.start():].strip()
                    else:
                        header = art_text.strip()
                        paras_text = ''
                    if header:
                        # strip leading article-number prefix so header shows only the title
                        header_clean = re.sub(
                            rf'^Artikel\s+{re.escape(num)}[\.:]?\s*', '', header
                        )
                        if header_clean:
                            out.write(header_clean + '\n\n')
                    if paras_text:
                        # find paragraph start positions (only at start or after semicolon delimiters)
                        para_matches = list(re.finditer(r'(?:(?<=^)|(?<=;\s))(\d+)\s+', paras_text))
                        for i, pm in enumerate(para_matches):
                            pnum = pm.group(1)
                            start = pm.end()
                            end = para_matches[i+1].start() if i+1 < len(para_matches) else len(paras_text)
                            ptext = paras_text[start:end].strip().rstrip(';')
                            # handle lettered subpoints
                            if ':' in ptext and re.search(r'\b[a-z]\.', ptext):
                                prefix, rest = ptext.split(':', 1)
                                letters = [x.strip().rstrip('.') for x in rest.split(';') if x.strip()]
                                out.write(f'{pnum}. {prefix.strip()}:\n\n')
                                for li in letters:
                                    m_li = re.match(r'([a-z])\.\s*(.*)', li)
                                    if m_li:
                                        out.write(f'    {m_li.group(1)}. {m_li.group(2).strip()}\n')
                                    else:
                                        out.write(f'    - {li}\n')
                                out.write('\n')
                            else:
                                out.write(f'{pnum}. {ptext}\n\n')
            if closing:
                out.write('## Origineel slotformulier en ondertekening\n\n')
                out.write(closing + '\n')
        print(f'Wrote {count} wetten to {outfile}')


if __name__ == '__main__':
    main()