package io.dataease.plugins.common.base.domain;

import java.io.Serializable;
import lombok.Data;

@Data
public class ExportTask implements Serializable {
    private String id;

    private Long userId;

    private String fileName;

    private Double fileSize;

    private String exportFrom;

    private String exportStatus;

    private String exportFromType;

    private Long exportTime;

    private String exportPogress;

    private static final long serialVersionUID = 1L;
}