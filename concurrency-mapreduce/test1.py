import os
from collections import Counter

def count_words_in_files(directory):
    word_count = Counter()
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        with open(file_path, 'r') as f:
            for line in f:
                words = line.split()
                word_count.update(words)
    return word_count

def compare_results(program_output, expected_output):
    with open(program_output, 'r') as f:
        program_counts = {}
        for line in f:
            word, count = line.strip(' ').split()
            program_counts[word] = int(count)

    for word, count in expected_output.items():
        if word not in program_counts or program_counts[word] != count:
            print(f"Mismatch for word '{word}': expected {count}, got {program_counts.get(word, 0)}")
            return False

    print("All counts match.")
    return True

if __name__ == "__main__":
    output_file = "program_output.txt"  # The file where your program writes its output
    expected_counts = count_words_in_files("test_files")
    compare_results(output_file, expected_counts)