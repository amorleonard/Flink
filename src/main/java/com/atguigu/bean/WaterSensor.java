package com.atguigu.bean;

import com.oracle.webservices.internal.api.databinding.DatabindingMode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName WaterSensor
 * @Description //TODO  添加lombok依赖
 * @Author Amor_leonard
 * @Date 2021/9/6 18:02
 * @Version 1.0
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
