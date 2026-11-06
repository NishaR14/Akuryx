\# PROMPTS.md



\## AI Tools Used



\*\*Tool:\*\* ChatGPT  

\*\*Purpose:\*\* Help with Go development, debugging, testing, and project documentation.



\*Note: This is a reconstructed log based on the available development conversation. The prompts below are paraphrased examples, not an exact verbatim export.\*



\### 1. Understanding the project



1\. "I have this hackathon problem statement. Can you go through it and tell me exactly what I need to build and what things will be checked by the judges?"

2\. "I am using Go and VS Code on Windows. Help me set up the project structure properly."

3\. "Can you explain the database requirements in simple terms? I am new to building something like this."



\### 2. Building the database



4\. "Help me design the file format for storing the key value data. I want it to be simple but also recover properly if the program crashes."

5\. "Can you help me implement the write ahead log in Go using only the standard library?"

6\. "How should I store transactions so that a batch is either fully saved or not saved at all?"

7\. "Please check my record encoding and decoding code. I think I missed some validation requirements."

8\. "The maximum key size is 1024 bytes. Can you update the validation without changing the rest of the working code?"



\### 3. Recovery and database operations



9\. "How do I make sure a successful put is still available after restarting the database?"

10\. "Can you check whether my WAL recovery handles an incomplete last record correctly?"

11\. "What should happen if the database file gets corrupted in the middle instead of just at the end?"

12\. "Help me check the batch implementation. I don't want readers to see only some of the batch operations."

13\. "I have added recovery tests. Can you help me understand which cases are still missing?"



\### 4. Compaction and file limits



14\. "My database file keeps growing even after deleting keys. How should I implement compaction?"

15\. "Can you explain the correct order for using a temporary file and backup file during compaction?"

16\. "What happens if the application crashes between renaming the old database and the new database?"

17\. "How can I test the maximum file size requirement and make sure old data is not lost when a write fails?"



\### 5. Testing and debugging



18\. "Can you give me commands to run all Go tests in PowerShell?"

19\. "My test is failing. I will paste the output. Please help me find the actual problem instead of changing unrelated code."

20\. "Can you help me test concurrent writers and readers using the race detector?"

21\. "I need to test one million puts and one million deletes. How can I verify that compaction actually reduces the file size?"

22\. "My disk full torture test is failing because of the error message matching. Can you help me fix that?"

23\. "How can I test recovery after killing the database process suddenly?"



\### 6. Torture testing



24\. "Can you review my torture test? It should start the database, send random operations, kill it, restart it and verify the acknowledged writes."

25\. "I need at least two writers in the crash test. Can you help me make sure both are running before the process is killed?"

26\. "The test should not trust the database dump alone. How should I compare it against an independent list of acknowledged operations?"

27\. "How do I run the torture test with a fixed seed so I can reproduce failures?"

28\. "Can you help me add separate tests for large values, large batches, recovery crashes, compaction and disk full?"



\### 7. Documentation and final checks



29\. "Help me write a README that explains how to build and run my database."

30\. "I need an architecture diagram for the project. Can you make one using Mermaid?"

31\. "Can you help me document the exact file format, write order, recovery process and locking?"

32\. "Please compare my BREAKING.md with the project requirements. I don't want to claim anything that I haven't actually tested."

33\. "I need to prepare PROMPTS.md. Help me organize the development questions in chronological order."

34\. "Let's check the project requirements one by one. If something is already passing, don't change it. Only work on the missing or failing parts."





