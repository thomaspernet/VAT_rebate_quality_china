import re, tex2pix, os
from PyPDF2 import PdfFileMerger
from wand.image import Image as WImage


def beautify(table_number,
 new_row = False,
  table_nte = None,
  jupyter_preview = True,
  resolution = 150):
    """
    """
    table_in = "table_{}.txt".format(table_number)
    table_out = "table_{}.tex".format(table_number)

    regex = \
    r"\s\simported\\_variety\:t\\_0"

    comp = r"\sherf\\_com\\_o"

    with open(table_in, "r") as f:
        lines = f.readlines()

        line_to_remove = []
    #return lines

    if table_nte!= None:
        max_ = 6
        max_1 = 9
    else:
        max_ =  8
        max_1 = 12
    for x, line in enumerate(lines[13:-max_]):
        test = bool(re.search(regex, line))
        test_1 = bool(re.search(comp, line))

        if test == True:
            line_to_remove.append(x + 13)
            line_to_remove.append((x + 13) + 1)

        ### Move down competition
        if test_1 == True:
            line_to_remove.append(x + 13)
            line_to_remove.append((x + 13) + 1)
            comp_coef = lines[13+x] + lines[13+x+1]
            comp_sd = lines[13+x+1]

    with open(table_out, "w") as f:
        for x, line in enumerate(lines):

            if x not in line_to_remove:
                f.write(line)

            ### move competition
            if x == len(lines) - max_1:
                f.write(comp_coef)
            #if x == len(lines) - 11:
            #    f.write(comp_sd)

    ### add ajdust box
    with open(table_out, 'r') as f:
        lines = f.readlines()

    if new_row != False:
        temp  = [' & '.join(new_row)]
        temp.append('\n \\\\[-1.8ex]')
        temp.append('\\\\\n ')
        #temp.append('\n \\\\[-1.8ex]\n')
        new_row_ = [temp[1] + temp[0] + temp[2] #+ temp[3]
        ]


    for x, line in enumerate(lines):
        label = bool(re.search(r"label",
                              line))
        tabluar = bool(re.search(r"end{tabular}",
                              line))
        if label:
            lines[x] = lines[x].strip() + '\n\\begin{adjustbox}{width=\\textwidth, totalheight=\\textheight-2\\baselineskip,keepaspectratio}\n'

        if tabluar:
            lines[x] = lines[x].strip() + '\n\\end{adjustbox}\n'

    for x, line in enumerate(lines):
        if x == 11:
            lines[x] = lines[x].strip() + new_row_[0]
            #lines[x+1] = lines[x].strip() + ''

    ### Add header
    len_line = len(lines)
    for x, line in enumerate(lines):
        if x ==1:
            if jupyter_preview:
                header= "\documentclass[preview]{standalone} \n\\usepackage[utf8]{inputenc}\n" \
            "\\usepackage{booktabs,caption,threeparttable, siunitx, adjustbox}\n\n" \
            "\\begin{document}"
            else:
                header= "\documentclass[12pt]{article} \n\\usepackage[utf8]{inputenc}\n" \
            "\\usepackage{booktabs,caption,threeparttable, siunitx, adjustbox}\n\n" \
            "\\begin{document}"

            lines[x] =  header

        if x == len_line- 1:
            footer = "\n\n\\end{document}"
            lines[x]  =  lines[x].strip() + footer

    with open(table_out, "w") as f:
        for line in lines:
            f.write(line)

                # Read in the file
    with open(table_out, 'r') as file:
        lines = file.read()

        #
        lines = lines.replace('eligibleNo:ln\\_vat\\_tax',
                              'Ln VAT export tax$_{k,t-1}$')

        lines = lines.replace('eligibleYes:ln\\_vat\\_tax',
                              'Ln VAT export tax$_{k,t-1}$ \\times \\text{ordinary}$_{i}$')
        lines = lines.replace('eligibleNo:ln\\_import\\_tax',
                              'Ln VAT import tax$_{k,t-1}$')
        lines = lines.replace('eligibleYes:ln\\_import\\_tax',
                              'Ln VAT import tax$_{k,t-1}$ \\times \\text{ordinary}$_{i}$')
        lines = lines.replace('herf\\_com\\_o',
                              'Competition$_{s,t-1}$')
        lines = lines.replace('ln\\_vat\\_tax:distance',
                              'Ln VAT export tax$_{k,t-1}$  x Density$_{ck}$')
        lines = lines.replace('ln\\_vat\\_tax:eligibleYes:distance',
                              'Ln VAT export tax$_{k,t-1}$ x Ordinary$_{i}$ x Density$_{ck}$')
        lines = lines.replace('ln\\_vat\\_tax:density\\_china\\_ville',
                      'Ln VAT export tax$_{k,t-1}$ x Density$_{ck}')

        lines = lines.replace('ln\\_vat\\_tax:balassa',
                      'Ln VAT export tax$_{k,t-1}$ x Comp Adv$_{ck}')

        lines = lines.replace('ln\\_vat\\_tax:Competition',
                      'Ln VAT export tax$_{k,t-1}$ x Competition$_{ck}')

        lines = lines.replace('ln\\_vat\\_tax:eligibleYes:density\\_china\\_ville',
                      'Ln VAT export tax$_{k,t-1}$ x Ordinary$_{i}$ x Density$_{ck}')


        lines = lines.replace('ln\\_vat\\_tax:eligibleYes:Competition',
                      'Ln VAT export tax$_{k,t-1}$ x Ordinary$_{i}$ x Competition$_{ck}$')

        lines = lines.replace('ln\\_vat\\_tax:eligibleYes:balassa',
                      'Ln VAT export tax$_{k,t-1}$ x Ordinary$_{i}$ x Comp Adv$_{ck}$')

        lines = lines.replace('ln\\_vat\\_tax',
                              'Ln VAT export tax$_{k,t-1}$')


    # Write the file out again
    with open(table_out, 'w') as file:
        file.write(lines)

    ### add table #
    if table_nte != None:
        with open(table_out, 'r') as f:
            lines = f.readlines()


        for x, line in enumerate(lines):
            adjusted = bool(re.search(r"end{adjustbox}",
                              line))

            if adjusted:
                lines[x] = lines[x].strip() + "\n\\begin{0} \n \\small \n \\item \\\\ \n{1} \n\\end{2}\n".format(
                "{tablenotes}",
                table_nte,
                "{tablenotes}")

        with open(table_out, "w") as f:
            for line in lines:
                f.write(line)

    if jupyter_preview:
        f = open('table_{}.tex'.format(table_number))
        r = tex2pix.Renderer(f, runbibtex=False)
        r.mkpdf('table_{}.pdf'.format(table_number))
        img = WImage(filename='table_{}.pdf'.format(table_number),
         resolution = resolution)
        return display(img)

    ### Add Adjust box
