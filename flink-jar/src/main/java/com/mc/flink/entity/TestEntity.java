package com.mc.flink.entity;

import lombok.Data;
import com.baomidou.mybatisplus.annotation.TableField;

/**
 * @author macheng
 * @date 2022/6/13 15:13
 */
@Data
public class TestEntity {
    @TableField
    private String test;
}
