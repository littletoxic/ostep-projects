import os
import random

def generate_test_files(num_files, words_per_file, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

        
    with open('/home/littletoxic/nltk_data/corpora/words/en-basic', 'r') as f:
        words1 = f.read().splitlines()
    with open('/home/littletoxic/nltk_data/corpora/words/en', 'r') as f:
        words2 = f.read().splitlines()
        
    for i in range(num_files):
        file_path = os.path.join(output_dir, f"file_{i}.txt")
        with open(file_path, "w") as f:
            for _ in range(words_per_file):
                if random.randint(1, 10) > 2:
                    word = random.choice(words1)
                else: 
                    word = random.choice(words2)
                f.write(word + " ")
            f.write("\n")

if __name__ == "__main__":
    num_files = 500  # Number of files to generate
    words_per_file = 5000  # Number of words per file
    output_dir = "test_files"
    generate_test_files(num_files, words_per_file, output_dir)
    print(f"Generated {num_files} files in '{output_dir}'")