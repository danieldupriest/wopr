#!/usr/local/bin/

# At top of consumer:
# Create toval list
# Create errors list
# Create toDB list
# Set FULL = 100
# At end of consumer loop, else clause:
# Append to toval
# When toval has FULL items, process:
    # Drop empty rows
    # Perform null validation 
    # Set datatypes
    # Validations
        # If row does not validate, add to errors list
    # Transformations
    # Add to toDB

toval = []
errors = []
toDB = []
FULL = 100


