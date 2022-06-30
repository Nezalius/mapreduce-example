/// Author Chris Baumann
use std::thread;

struct WordCount {
    word_string: String,
    count: i32,
}


fn read_file(filename: String) -> String {
    let contents_of_file: String = std::fs::read_to_string(filename).expect("Something went wrong");
    contents_of_file
}

fn divide_string_to_vec(text: String) -> Vec<String> {
    let mut all_words: Vec<&str> = text.split_whitespace().collect();
    let mut all_words_string: Vec<String> = Vec::new();
    for _i in 0..all_words.len() {
        all_words_string.push(all_words.remove(0).to_string());
    }
    all_words_string
}

fn divide_for_threading(text:&mut Vec<String>, amount_of_threads: &mut usize) -> Vec<Vec<String>> {
    let mut words_per_sublist: usize;
    if *amount_of_threads == 15 as usize {
        *amount_of_threads = 10;
        words_per_sublist = text.len() / *amount_of_threads + 1;
        words_per_sublist = words_per_sublist - (words_per_sublist % 10)
    } else {
        words_per_sublist = text.len() / *amount_of_threads;    
    }
    let mut words_in_sublist: u32 = 0;
    let mut curr_sublist: usize = 0;
    let mut result: Vec<Vec<String>> = vec![amount_of_threads; vec![]];
    for _i in 0..text.len() {
        if words_in_sublist > words_per_sublist as u32 {
            words_in_sublist = 0;
            curr_sublist += 1;
        }
        if curr_sublist >= result.len() {
            result.push(vec![]);
        }
        result[curr_sublist].push(text.remove(0));
        words_in_sublist += 1;
    }
    result
}

fn main_phase(data:&mut Vec<String>) -> Vec<WordCount> {
    let mut result: Vec<WordCount> = Vec::new();
    for _i in 0..data.len() {
        let word_count = WordCount {word_string: data.remove(0), count: 1};
        result.push(word_count);
    }
    result
}

fn reduce_phase(data: &mut Vec<WordCount>) -> Vec<WordCount> {
    let mut result: Vec<WordCount> = Vec::new();
    for _i in 0..data.len() {
        let mut existing: bool = false;
        for j in 0..result.len() {
            if data[0].word_string == result[j].word_string && existing == false {
                result[j].count += 1;
                existing = true;
                data.remove(0);
            }
        }
        if existing == false {
            let val: WordCount = data.remove(0);
            result.push(WordCount { word_string: val.word_string, count: val.count });
        }
    }
    result
}

fn number_threads(length_of_text: usize) -> usize {
    let mut result: usize = 0;
    for i in 5..11 {
        if length_of_text % i as usize == 0 {
            result = i;
        }
    }
    if result == 0 {
        result = 15;
    }
    result
}

fn main() {
    

    // example text
    let mut divided_text = divide_string_to_vec(read_file("src/test.txt".to_string()));
    let mut thread_count: usize = number_threads(divided_text.len());
    let mut usable_text: Vec<Vec<String>> = divide_for_threading(&mut divided_text, &mut thread_count);
    
    
    /*********************************************************************************************
     * Map phase -> create structs WordCount with word (as "key") and a 1, for each orccurence of a word
     *********************************************************************************************/
    
     // vector for child threats spawned
    let mut children= vec![];

 
    for _i in 0..usable_text.len() {
        let mut curr_sub_vec: Vec<String> = usable_text.remove(0);
        children.push(thread::spawn(move || -> Vec<WordCount> {
            let result: Vec<WordCount> = main_phase(&mut curr_sub_vec);
            result
        }));

    }

    // vector as container to join all threads together
    let mut intermediary: Vec<WordCount> = vec![];

    // put all WordCounts in a single vector
    for _child in 0..children.len() {
        let mut curr_vec = children.remove(0).join().unwrap();
        for _i in 0..curr_vec.len() {
            intermediary.push(curr_vec.remove(0));
        }
    }


    // showcase the output of the Map Phase
    for i in 0..intermediary.len() {
        println!("{}: {}", intermediary[i].word_string, intermediary[i].count);
    }
    

    /****************************************************************************************
     * Reduce phase -> take all words and count the 1 parts to get the final number of occurences of a word
     * => collect the words in a vector with their occurences as partner based on struct WordCount
     ****************************************************************************************/

    let final_result: Vec<WordCount> = reduce_phase(&mut intermediary);

    // output each word and the number of its occurences
    for word in 0..final_result.len() {
        println!("Anzahl des Wortes {}: {}", final_result[word].word_string, final_result[word].count);
    }
}
