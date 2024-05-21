# GAS Framework
An enhanced web framework (based on [Flask](https://flask.palletsprojects.com/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](https://getbootstrap.com/docs/3.3/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files


### Approach

1. **Sending Message to SQS Topic:**
   - The process begins with sending a message to the designated SQS topic when finishing annotate the task file.

2. **Time Difference Check:**
   - Upon receiving the message including the completion time, the system calculates the time difference between the completion time and the current time to check if it last 3 mins.

3. **Condition Check:**
   - If the time difference is greater than 3 minutes:
     - Proceed to check the user profile to determine if they are a free user or not.
       - If the user is identified as a free user:
         - Move the result file to S3 Glacier.
         - Delete the result file from S3.
         - update db with Glacier ID
         - delete the message
       - If the user is identified as Premium:
         - delete the message
   - If the time difference is less than 3 minutes:
     - ignore the message, poll a new message(old message can been polled again, no message will be lost)


4. **Helper Function Execution:**
   - A helper function, `get_profile`, is invoked to retrieve user information.

5. **Action Execution:**
   - Actions such as moving files to Glacier and deleting files from S3 are executed based on the condition evaluation results.

### Rationale

1. **Scalability:**
   - The approach considers scalability by utilizing AWS services like SQS, S3, and Glacier, which are designed to handle varying workloads efficiently.
   - By leveraging SQS, the system can decouple message producers from consumers, allowing for better scalability and fault tolerance.

2. **Consistency:**
   - Consistency is maintained by ensuring that actions are taken based on predefined conditions consistently.
   - The use of SQS ensures that messages are processed in the order they are received, maintaining consistency in processing.

3. **Efficiency:**
   - The time difference check ensures that resources are utilized efficiently. Actions are only taken when necessary, reducing unnecessary processing and resource consumption.


By following this approach, the system aims to achieve scalability, consistency, efficiency, and cost optimization while ensuring reliability and flexibility in handling message processing and actions.