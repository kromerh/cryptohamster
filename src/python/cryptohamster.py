
# Logic

# Loop every second

# Read the session table

# If a session is running

# Read the decision table for that session, retrieve the latest entry
# If a decision is open (latest decision has no end_time)
# Check for timeout, if the last reading in the wheel table is older than 5 s, a decision was reached
# Count the number of turns of the wheel between the start and the end hamsterwheel id
# Use the count to determine the decision outcome
# Update the decision table (result, end_time, hamsterwheel_id_start)

# If no decision is running (latest decision has end_time)
# Check what the latest decision was
# If the latest decision type was amount begin the trade logic
# Get the amount to buy or sell
# Get the currency to buy or sell and retrieve the current price from binance
# Register the order in the tradebook
# Update the wallet
# If the latest decision type was not amount move to the next decision type


# Else no session is running
# If the last reading in the wheel table is newer than 5 s, a decision will be started
# In this case create a new session
# Kick off the decision pipeline aka move to the next decision type

# If the last reading in the wheel table is older than 5 s, do nothing, move to the next loop iteration

# For moving to the next decision type:
# If the next decision is buy_sell
# Check if the hamster has cash and/or currencies
# If the hamster has no cash but holds currencies: decision can only be sell
# In this case update the decision table
# Move to the next loop iteration
# If the hamster has cash but holds no currencies: decision can only be buy
# In this case update the decision table
# Move to the next loop iteration
