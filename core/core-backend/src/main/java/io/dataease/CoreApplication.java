package io.dataease;

import io.dataease.license.config.LicenseConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.quartz.QuartzAutoConfiguration;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = {QuartzAutoConfiguration.class})
@EnableCaching
@EnableScheduling
@ComponentScan(excludeFilters = {
//        @ComponentScan.Filter(
//        type = FilterType.REGEX,
//        pattern = {
//               "io.dataease.*.LicSw"
//        }),
        @ComponentScan.Filter(
                type = FilterType.ASSIGNABLE_TYPE,
                classes = {LicenseConfig.class}
        )
//        ,
//        @ComponentScan.Filter(
//                type = FilterType.ASSIGNABLE_TYPE,
//                classes = {FilterConfig.class}
//        )
})
public class CoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(CoreApplication.class, args);
    }
}
