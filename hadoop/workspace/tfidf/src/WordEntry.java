
public class WordEntry implements Comparable<WordEntry>{
	String key;
	double value;

    public WordEntry(String key, double value) {
        super();
        this.key = key;
        this.value = value;
    }

    public int compareTo(WordEntry o) {
    	int res = Double.compare(this.value, o.value);
        return (res == 0) ? this.key.compareTo(o.key) : res;
    }
    
    public String toString() {
    	return "{" + key + "=" + value + "}";
    }
}