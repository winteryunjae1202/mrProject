package cache;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 리듀스 역할을 수행하기 위해서는 Reducer 자바 파일을 상속받아야함
 * Reducer 파일의 앞의 2개 데이터 타입(Text, IntWritable)은 Suffle and Sort에 보낸 데이터의 키와 값의 데이터 타입
 * 보통 Mapper에서 보낸 데이터 타입과 동일함
 * Reducer 파일의 뒤의 2개 데이터 타입(Text, IntWritable)은 경과 파일 생성에 사용될 키와 값의 데이터 타입
 */
public class WordCount3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행됨
     * 파일의 라인수가 100개라면, map함수는 100번 실행됨
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 단어별 빈도수를 계산하기 위한 변수
        int wordCount = 0;

        // Suffle and Sort로 인해 단어별로 데이터들의 값들이 List 구조로 저장됨
        // 모든 값은 1이기에 모두 더하기 해도됨
        for (IntWritable value : values) {
            // 값을 모두 더하기
            wordCount += value.get();
        }

        // 분석 결과 파일에 데이터 저장하기
        context.write(key, new IntWritable(wordCount));
    }
}
