//! SQL处理器组件
//!
//! 使用DataFusion执行SQL查询处理数据，支持静态SQL和流式SQL

use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::SchemaExt;

use crate::{Error, Message, processor::Processor};

/// 窗口类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowType {
    /// 滚动窗口（固定大小，不重叠）
    Tumbling,
    /// 滑动窗口（固定大小，可重叠）
    Sliding,
    /// 会话窗口（由不活动间隔定义）
    Session,
}

/// 窗口配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowConfig {
    /// 窗口类型
    pub window_type: WindowType,
    /// 窗口大小（毫秒）
    pub size_ms: u64,
    /// 窗口滑动步长（毫秒，仅用于滑动窗口）
    pub slide_ms: Option<u64>,
    /// 会话超时（毫秒，仅用于会话窗口）
    pub timeout_ms: Option<u64>,
    /// 时间戳字段名
    pub timestamp_field: String,
    /// 水印延迟（毫秒）
    pub watermark_delay_ms: u64,
}

/// SQL处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    /// SQL查询语句
    pub query: String,

    /// 表名（用于SQL查询中引用）
    pub table_name: String,

    /// 窗口配置（可选，用于流式SQL）
    pub window: Option<WindowConfig>,
    /// 状态保留时间（毫秒，0表示无限，仅用于流式SQL）
    pub state_ttl_ms: Option<u64>,
    /// 目标字段（可选，用于将结果存储到特定字段）
    pub target: Option<String>,
}

/// SQL处理器状态
struct SqlState {
    /// 会话上下文
    ctx: SessionContext,
    /// 窗口数据缓存
    window_data: Vec<RecordBatch>,
    /// 最后处理的时间戳
    last_timestamp: i64,
    /// 状态数据（用于聚合等）
    state_data: HashMap<String, serde_json::Value>,
    /// 最后状态更新时间
    last_state_update: std::time::Instant,
}

/// SQL处理器组件
pub struct SqlProcessor {
    config: SqlProcessorConfig,
    /// 流式SQL状态（仅在流式模式下使用）
    state: Option<Arc<Mutex<SqlState>>>,
}

impl SqlProcessor {
    /// 创建一个新的SQL处理器组件
    pub fn new(config: &SqlProcessorConfig) -> Result<Self, Error> {
        // 检查是否为流式SQL模式
        let state = if config.window.is_some() {
            // 创建初始状态
            let sql_state = SqlState {
                ctx: SessionContext::new(),
                window_data: Vec::new(),
                last_timestamp: 0,
                state_data: HashMap::new(),
                last_state_update: std::time::Instant::now(),
            };
            Some(Arc::new(Mutex::new(sql_state)))
        } else {
            None
        };

        Ok(Self {
            config: config.clone(),
            state,
        })
    }

    /// 将消息内容解析为DataFusion表
    async fn parse_input(&self, content: &str) -> Result<RecordBatch, Error> {
        self.parse_json_input(content).await
    }

    /// 解析JSON输入
    async fn parse_json_input(&self, content: &str) -> Result<RecordBatch, Error> {
        // 解析JSON内容
        let json_value: serde_json::Value = serde_json::from_str(content)
            .map_err(|e| Error::Processing(format!("JSON解析错误: {}", e)))?;

        // 处理不同的JSON结构
        match json_value {
            serde_json::Value::Object(obj) => {
                // 单个对象转换为单行表
                let mut fields = Vec::new();
                let mut columns: Vec<ArrayRef> = Vec::new();

                // 提取所有字段和值
                for (key, value) in obj {
                    fields.push(Field::new(&key, DataType::Utf8, false));

                    // 将值转换为字符串
                    let str_value = match value {
                        serde_json::Value::Null => "null".to_string(),
                        _ => value.to_string(),
                    };

                    // 创建列数据
                    let array = StringArray::from(vec![str_value]);
                    columns.push(Arc::new(array));
                }

                // 创建schema和记录批次
                let schema = Arc::new(Schema::new(fields));
                RecordBatch::try_new(schema, columns)
                    .map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)))
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    // 返回一个空的记录批次而不是错误
                    let schema = Arc::new(Schema::new(vec![] as Vec<Field>));
                    return RecordBatch::try_new(schema, vec![]).map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)));
                }

                // 数组的第一个元素用于确定schema
                if let Some(serde_json::Value::Object(first_obj)) = arr.first() {
                    let mut fields = Vec::new();
                    let mut columns: Vec<Vec<String>> = Vec::new();

                    // 从第一个对象提取字段
                    for key in first_obj.keys() {
                        fields.push(Field::new(key, DataType::Utf8, false));
                        columns.push(Vec::with_capacity(arr.len()));
                    }

                    // 填充所有行的数据
                    for item in &arr {
                        if let serde_json::Value::Object(obj) = item {
                            let mut col_idx = 0;
                            for key in first_obj.keys() {
                                let value = obj.get(key).unwrap_or(&serde_json::Value::Null);
                                let str_value = match value {
                                    serde_json::Value::Null => "null".to_string(),
                                    _ => value.to_string(),
                                };
                                columns[col_idx].push(str_value);
                                col_idx += 1;
                            }
                        } else {
                            // 跳过非对象元素而不是返回错误
                            continue;
                        }
                    }

                    // 如果所有元素都被跳过，返回空的记录批次
                    if columns.first().map_or(true, |col| col.is_empty()) {
                        let schema = Arc::new(Schema::new(vec![] as Vec<Field>));
                        return RecordBatch::try_new(schema, vec![]).map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)));
                    }

                    // 创建Arrow列
                    let arrow_columns: Vec<ArrayRef> = columns.iter()
                        .map(|col| Arc::new(StringArray::from(col.clone())) as ArrayRef)
                        .collect();

                    // 创建schema和记录批次
                    let schema = Arc::new(Schema::new(fields));
                    RecordBatch::try_new(schema, arrow_columns)
                        .map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)))
                } else {
                    Err(Error::Processing("JSON数组的第一个元素不是对象".to_string()))
                }
            }
            _ => Err(Error::Processing("输入必须是JSON对象或数组".to_string())),
        }
    }



    /// 执行SQL查询
    async fn execute_query(&self, batch: RecordBatch) -> Result<RecordBatch, Error> {
        // 创建会话上下文
        let ctx = SessionContext::new();

        // 注册表
        ctx.register_batch(&self.config.table_name, batch)
            .map_err(|e| Error::Processing(format!("注册表失败: {}", e)))?;

        // 执行SQL查询
        let df = ctx.sql(&self.config.query).await
            .map_err(|e| Error::Processing(format!("SQL查询错误: {}", e)))?;

        // 收集结果
        let result_batches = df.collect().await
            .map_err(|e| Error::Processing(format!("收集查询结果错误: {}", e)))?;

        if result_batches.is_empty() {
            return Err(Error::Processing("查询结果为空".to_string()));
        }

        Ok(result_batches[0].clone())
    }

    /// 将查询结果格式化为输出
    fn format_output(&self, batch: &RecordBatch) -> Result<String, Error> {
        self.format_json_output(batch)
    }

    /// 格式化为JSON输出
    fn format_json_output(&self, batch: &RecordBatch) -> Result<String, Error> {
        let schema = batch.schema();
        let mut result = Vec::new();

        // 遍历每一行
        for row_idx in 0..batch.num_rows() {
            let mut row_obj = serde_json::Map::new();

            // 遍历每一列
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let field_name = schema.field(col_idx).name();

                // 获取单元格值并转换为JSON值
                let value = if column.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    // 提取字符串值
                    let display_value = if let Some(s) = format!("{:?}", column.as_ref()).strip_prefix("StringArray\n[") {
                        if let Some(end) = s.strip_suffix("]") {
                            let values: Vec<&str> = end.split(",").collect();
                            if row_idx < values.len() {
                                values[row_idx].trim().trim_matches('"').to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    } else {
                        // 尝试其他格式的数组
                        let array_str = format!("{:?}", column.as_ref());
                        if array_str.contains("[") && array_str.contains("]") {
                            let start_idx = array_str.find("[").unwrap_or(0) + 1;
                            let end_idx = array_str.find("]").unwrap_or(array_str.len());
                            if start_idx < end_idx {
                                let content = &array_str[start_idx..end_idx];
                                let values: Vec<&str> = content.split(",").collect();
                                if row_idx < values.len() {
                                    values[row_idx].trim().trim_matches('"').to_string()
                                } else {
                                    "".to_string()
                                }
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    };

                    // 尝试将值解析为JSON，如果失败则作为字符串处理
                    if display_value.starts_with('{') && display_value.ends_with('}') ||
                        display_value.starts_with('[') && display_value.ends_with(']') {
                        match serde_json::from_str(&display_value) {
                            Ok(json_value) => json_value,
                            Err(_) => serde_json::Value::String(display_value)
                        }
                    } else if display_value == "null" {
                        serde_json::Value::Null
                    } else if let Ok(num) = display_value.parse::<i64>() {
                        serde_json::Value::Number(serde_json::Number::from(num))
                    } else if let Ok(num) = display_value.parse::<f64>() {
                        match serde_json::Number::from_f64(num) {
                            Some(n) => serde_json::Value::Number(n),
                            None => serde_json::Value::String(display_value)
                        }
                    } else if display_value == "true" {
                        serde_json::Value::Bool(true)
                    } else if display_value == "false" {
                        serde_json::Value::Bool(false)
                    } else {
                        serde_json::Value::String(display_value)
                    }
                };

                row_obj.insert(field_name.clone(), value);
            }

            result.push(serde_json::Value::Object(row_obj));
        }

        // 如果只有一行，返回对象而不是数组
        let final_result = if result.len() == 1 {
            result.pop().unwrap()
        } else {
            serde_json::Value::Array(result)
        };

        serde_json::to_string(&final_result)
            .map_err(|e| Error::Processing(format!("JSON序列化错误: {}", e)))
    }




    /// 提取时间戳
    fn extract_timestamp(&self, batch: &RecordBatch, window_config: &WindowConfig) -> Result<i64, Error> {
        let schema = batch.schema();
        let timestamp_field = &window_config.timestamp_field;

        // 查找时间戳字段的索引
        let col_idx = schema.fields().iter().position(|f| f.name() == timestamp_field)
            .ok_or_else(|| Error::Processing(format!("时间戳字段不存在: {}", timestamp_field)))?;

        // 获取时间戳值
        let column = batch.column(col_idx);
        if batch.num_rows() == 0 {
            return Err(Error::Processing("批次中没有行".to_string()));
        }

        // 获取第一行的时间戳（假设所有行的时间戳相近）
        let ts_str = if let Some(s) = format!("{:?}", column.as_ref()).strip_prefix("StringArray\n[") {
            if let Some(end) = s.strip_suffix("]") {
                let values: Vec<&str> = end.split(",").collect();
                if !values.is_empty() {
                    values[0].trim().to_string().trim_matches('"').to_string()
                } else {
                    return Err(Error::Processing("无法解析时间戳数组".to_string()));
                }
            } else {
                return Err(Error::Processing("无法解析时间戳数组格式".to_string()));
            }
        } else {
            // 尝试其他格式的数组
            let array_str = format!("{:?}", column.as_ref());
            if array_str.contains("[") && array_str.contains("]") {
                let start_idx = array_str.find("[").unwrap_or(0) + 1;
                let end_idx = array_str.find("]").unwrap_or(array_str.len());
                if start_idx < end_idx {
                    let content = &array_str[start_idx..end_idx];
                    let values: Vec<&str> = content.split(",").collect();
                    if !values.is_empty() {
                        values[0].trim().to_string().trim_matches('"').to_string()
                    } else {
                        return Err(Error::Processing("无法解析时间戳数组内容".to_string()));
                    }
                } else {
                    return Err(Error::Processing("无法解析时间戳数组范围".to_string()));
                }
            } else {
                return Err(Error::Processing(format!("无法识别时间戳列格式: {}", array_str)));
            }
        };

        // 尝试将时间戳解析为毫秒级整数
        ts_str.parse::<i64>()
            .map_err(|e| Error::Processing(format!("无法解析时间戳: {} - {}", ts_str, e)))
    }

    /// 应用窗口逻辑
    async fn apply_window(&self, batch: RecordBatch) -> Result<Option<RecordBatch>, Error> {
        let state = match &self.state {
            Some(s) => s,
            None => return Ok(Some(batch)), // 非流式模式，直接返回批次
        };

        let mut state_guard = state.lock().await;

        // 如果没有配置窗口，直接返回批次
        let window_config = match &self.config.window {
            Some(config) => config,
            None => return Ok(Some(batch)),
        };

        // 提取时间戳
        let timestamp = self.extract_timestamp(&batch, window_config)?;

        // 添加批次到窗口数据
        state_guard.window_data.push(batch);

        let should_trigger = match window_config.window_type {
            WindowType::Tumbling => {
                // 计算当前窗口的结束时间
                let window_size = window_config.size_ms as i64;
                let window_end = (timestamp / window_size + 1) * window_size;

                // 如果当前时间戳加上水印延迟超过了窗口结束时间，触发窗口计算
                timestamp + window_config.watermark_delay_ms as i64 >= window_end
            }
            WindowType::Sliding => {
                // 滑动窗口的滑动步长
                let slide_ms = window_config.slide_ms.unwrap_or(window_config.size_ms) as i64;

                // 计算当前触发点
                let trigger_point = (timestamp / slide_ms + 1) * slide_ms;

                // 如果当前时间戳加上水印延迟超过了触发点，触发窗口计算
                timestamp + window_config.watermark_delay_ms as i64 >= trigger_point
            }
            WindowType::Session => {
                // 会话窗口的超时时间
                let timeout_ms = window_config.timeout_ms.unwrap_or(30000) as i64;

                // 如果自上次处理以来已经过了超时时间，触发窗口计算
                state_guard.last_timestamp > 0 && timestamp - state_guard.last_timestamp > timeout_ms
            }
        };

        // 更新最后处理的时间戳
        if timestamp > state_guard.last_timestamp {
            state_guard.last_timestamp = timestamp;
        }

        if should_trigger {
            let result = self.process_window_data(&mut state_guard).await?;
            return Ok(result);
        }

        // 如果没有触发窗口计算，返回None
        Ok(None)
    }

    /// 处理窗口数据
    async fn process_window_data(&self, state: &mut SqlState) -> Result<Option<RecordBatch>, Error> {
        if state.window_data.is_empty() {
            return Ok(None);
        }

        // 合并所有窗口数据
        let combined_batch = self.combine_batches(&state.window_data)?;

        // 清空窗口数据
        state.window_data.clear();

        // 执行SQL查询
        let result = match self.execute_query(combined_batch).await {
            Ok(batch) => batch,
            Err(e) => {
                // 如果是空结果错误，返回None而不是错误
                if let Error::Processing(msg) = &e {
                    if msg == "查询结果为空" {
                        return Ok(None);
                    }
                }
                return Err(e);
            }
        };

        // 更新状态数据
        self.update_state_data(state, &result).await?;

        Ok(Some(result))
    }

    /// 合并多个记录批次
    fn combine_batches(&self, batches: &[RecordBatch]) -> Result<RecordBatch, Error> {
        if batches.is_empty() {
            return Err(Error::Processing("没有批次可合并".to_string()));
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        // 使用第一个批次的schema
        let schema = batches[0].schema();

        // 为每一列创建合并数据
        let mut combined_columns: Vec<Vec<String>> = Vec::new();
        for _ in 0..schema.fields().len() {
            combined_columns.push(Vec::new());
        }

        // 合并所有批次的数据
        for batch in batches {
            if !batch.schema().logically_equivalent_names_and_types(&schema) {
                return Err(Error::Processing("批次schema不一致".to_string()));
            }

            for row_idx in 0..batch.num_rows() {
                for col_idx in 0..batch.num_columns() {
                    if col_idx >= combined_columns.len() {
                        // 安全检查，确保列索引有效
                        continue;
                    }

                    let column = batch.column(col_idx);
                    let value = if column.is_null(row_idx) {
                        "null".to_string()
                    } else {
                        if let Some(s) = format!("{:?}", column.as_ref()).strip_prefix("StringArray\n[") {
                            if let Some(end) = s.strip_suffix("]") {
                                let values: Vec<&str> = end.split(",").collect();
                                if row_idx < values.len() {
                                    values[row_idx].trim().trim_matches('"').to_string()
                                } else {
                                    "".to_string()
                                }
                            } else {
                                "".to_string()
                            }
                        } else {
                            // 尝试其他格式的数组
                            let array_str = format!("{:?}", column.as_ref());
                            if array_str.contains("[") && array_str.contains("]") {
                                let start_idx = array_str.find("[").unwrap_or(0) + 1;
                                let end_idx = array_str.find("]").unwrap_or(array_str.len());
                                if start_idx < end_idx {
                                    let content = &array_str[start_idx..end_idx];
                                    let values: Vec<&str> = content.split(",").collect();
                                    if row_idx < values.len() {
                                        values[row_idx].trim().trim_matches('"').to_string()
                                    } else {
                                        "".to_string()
                                    }
                                } else {
                                    "".to_string()
                                }
                            } else {
                                "".to_string()
                            }
                        }
                    };
                    combined_columns[col_idx].push(value);
                }
            }
        }

        // 创建Arrow列
        let arrow_columns: Vec<ArrayRef> = combined_columns.iter()
            .map(|col| Arc::new(StringArray::from(col.clone())) as ArrayRef)
            .collect();

        // 创建合并的记录批次
        RecordBatch::try_new(schema, arrow_columns)
            .map_err(|e| Error::Processing(format!("创建合并批次失败: {}", e)))
    }

    /// 更新状态数据
    async fn update_state_data(&self, state: &mut SqlState, batch: &RecordBatch) -> Result<(), Error> {
        // 更新状态更新时间
        state.last_state_update = std::time::Instant::now();

        // 如果配置了状态TTL，清理过期状态
        if let Some(ttl_ms) = self.config.state_ttl_ms {
            if ttl_ms > 0 {
                self.clean_expired_state(state, ttl_ms).await?;
            }
        }

        // 将批次数据转换为状态数据
        let schema = batch.schema();

        for row_idx in 0..batch.num_rows() {
            // 使用第一列作为键（通常是分组键）
            if batch.num_columns() < 2 {
                continue; // 需要至少两列：键和值
            }

            let key_column = batch.column(0);
            let value_column = batch.column(1);

            let key = if let Some(s) = format!("{:?}", key_column.as_ref()).strip_prefix("StringArray\n[") {
                if let Some(end) = s.strip_suffix("]") {
                    let values: Vec<&str> = end.split(",").collect();
                    if row_idx < values.len() {
                        values[row_idx].trim().trim_matches('"').to_string()
                    } else {
                        continue; // 跳过无效行
                    }
                } else {
                    continue; // 跳过无效行
                }
            } else {
                // 尝试其他格式的数组
                let array_str = format!("{:?}", key_column.as_ref());
                if array_str.contains("[") && array_str.contains("]") {
                    let start_idx = array_str.find("[").unwrap_or(0) + 1;
                    let end_idx = array_str.find("]").unwrap_or(array_str.len());
                    if start_idx < end_idx {
                        let content = &array_str[start_idx..end_idx];
                        let values: Vec<&str> = content.split(",").collect();
                        if row_idx < values.len() {
                            values[row_idx].trim().trim_matches('"').to_string()
                        } else {
                            continue; // 跳过无效行
                        }
                    } else {
                        continue; // 跳过无效行
                    }
                } else {
                    continue; // 跳过无效行
                }
            };

            let value = if value_column.is_null(row_idx) {
                serde_json::Value::Null
            } else {
                // 尝试将值解析为JSON，如果失败则作为字符串处理
                let value_str = if let Some(s) = format!("{:?}", value_column.as_ref()).strip_prefix("StringArray\n[") {
                    if let Some(end) = s.strip_suffix("]") {
                        let values: Vec<&str> = end.split(",").collect();
                        if row_idx < values.len() {
                            values[row_idx].trim().trim_matches('"').to_string()
                        } else {
                            "".to_string()
                        }
                    } else {
                        "".to_string()
                    }
                } else {
                    // 尝试其他格式的数组
                    let array_str = format!("{:?}", value_column.as_ref());
                    if array_str.contains("[") && array_str.contains("]") {
                        let start_idx = array_str.find("[").unwrap_or(0) + 1;
                        let end_idx = array_str.find("]").unwrap_or(array_str.len());
                        if start_idx < end_idx {
                            let content = &array_str[start_idx..end_idx];
                            let values: Vec<&str> = content.split(",").collect();
                            if row_idx < values.len() {
                                values[row_idx].trim().trim_matches('"').to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    } else {
                        "".to_string()
                    }
                };

                // 尝试将值解析为JSON，如果失败则作为字符串处理
                if value_str.starts_with('{') && value_str.ends_with('}') ||
                    value_str.starts_with('[') && value_str.ends_with(']') {
                    match serde_json::from_str(&value_str) {
                        Ok(json_value) => json_value,
                        Err(_) => serde_json::Value::String(value_str)
                    }
                } else if value_str == "null" {
                    serde_json::Value::Null
                } else if let Ok(num) = value_str.parse::<i64>() {
                    serde_json::Value::Number(serde_json::Number::from(num))
                } else if let Ok(num) = value_str.parse::<f64>() {
                    match serde_json::Number::from_f64(num) {
                        Some(n) => serde_json::Value::Number(n),
                        None => serde_json::Value::String(value_str)
                    }
                } else if value_str == "true" {
                    serde_json::Value::Bool(true)
                } else if value_str == "false" {
                    serde_json::Value::Bool(false)
                } else {
                    serde_json::Value::String(value_str)
                }
            };

            // 更新状态数据
            state.state_data.insert(key, value);
        }

        Ok(())
    }

    /// 清理过期状态
    async fn clean_expired_state(&self, state: &mut SqlState, ttl_ms: u64) -> Result<(), Error> {
        if ttl_ms == 0 {
            return Ok(()); // 不清理
        }

        let ttl_duration = std::time::Duration::from_millis(ttl_ms);
        let now = std::time::Instant::now();

        // 如果自上次状态更新以来的时间超过TTL，清空所有状态
        if now.duration_since(state.last_state_update) > ttl_duration {
            state.state_data.clear();
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // SQL处理器不需要特殊的关闭操作
        Ok(())
    }
}

#[async_trait]
impl Processor for SqlProcessor {
    async fn process(&self, mut msg: Message) -> Result<Vec<Message>, Error> {
        // 获取消息内容
        let content = msg.as_string()?;

        // 解析输入数据为DataFusion表
        let input_batch = self.parse_input(&content).await?;

        // 根据是否配置了窗口，决定使用静态SQL还是流式SQL处理
        let result_batch = if self.config.window.is_some() {
            // 流式SQL处理 - 应用窗口逻辑
            match self.apply_window(input_batch).await? {
                Some(batch) => batch,
                None => return Ok(vec![]), // 如果没有输出批次，返回空结果
            }
        } else {
            // 静态SQL处理 - 直接执行查询
            self.execute_query(input_batch).await?
        };

        // 格式化结果
        let result_str = self.format_output(&result_batch)?;

        // 如果指定了目标字段，则将结果添加到元数据
        if let Some(target) = &self.config.target {
            msg.metadata_mut().set(target, &result_str);
        } else {
            // 否则，将结果设置为消息内容
            msg.set_content(result_str.into_bytes());
        }

        Ok(vec![msg])
    }

    async fn close(&self) -> Result<(), Error> {
        // SQL处理器不需要特殊的关闭操作
        Ok(())
    }
}