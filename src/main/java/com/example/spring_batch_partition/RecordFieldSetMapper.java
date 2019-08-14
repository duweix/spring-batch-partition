package com.example.spring_batch_partition;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class RecordFieldSetMapper implements FieldSetMapper<Transaction> {

    @Override
    public Transaction mapFieldSet(FieldSet fieldSet) throws BindException {
        // TODO 自動生成されたメソッド・スタブ
        return null;
    }

}
