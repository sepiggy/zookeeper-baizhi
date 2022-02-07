package cn.sepiggy.zk.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

//! 使用 JDK 方式序列化必须实现序列化接口
@Setter
@Getter
@ToString
public class User implements Serializable {

    private Integer id;
    private String name;
    private Integer age;
    private Date bir;
}
