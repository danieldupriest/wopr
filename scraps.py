
'''
ap = argparse.ArgumentParser()
ap.add_argument(
    "-p", "--percentage",
    help="Show percentage of missing data for each column rather than raw total.",
    type=bool,
    default=False,
)
args = ap.parse_args()
'''

#TODO Why did this only work for the 2nd row???
    '''
    # If command line arg for percentage is True, use percentage instead of raw total:
    if args.percentage is True:
        for i in range(0, len(num_rows)): 
            for j in range(0, len(values)):
                values[j] = round(values[j] * 100 / num_rows[i], 2)
    '''

