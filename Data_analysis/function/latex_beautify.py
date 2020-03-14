import re


def beautify(table_number, new_row = False):
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

    max_ =  8
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
            comp_coef = lines[13+x]
            comp_sd = lines[13+x+1]

    print(line_to_remove)

    with open(table_out, "w") as f:
        for x, line in enumerate(lines):

            if x not in line_to_remove:
                f.write(line)

            ### move competition
            if x == len(lines) - 12:
                print(x, line, comp_coef)
                f.write(comp_coef)
            if x == len(lines) - 11:
                f.write(comp_sd)

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

    with open(table_out, "w") as f:
        for line in lines:
            f.write(line)

                # Read in the file
    with open(table_out, 'r') as file:
        lines = file.read()

        #
        lines = lines.replace('eligibleNo:ln\\_vat\\_tax',
                              'Ln VAT export tax$_{k,t-1}$')

        lines = lines.replace('ln\\_vat\\_tax',
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

        #lines = lines.replace(':', ' \\times ')

    # Write the file out again
    with open(table_out, 'w') as file:
        file.write(lines)

    ### Add Adjust box
