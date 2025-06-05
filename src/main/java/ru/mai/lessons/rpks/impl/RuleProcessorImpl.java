package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Message processing(Message message, Rule[] rules) {
    try {
      JsonNode jsonNode = mapper.readTree(message.getValue());
      if (rules == null || rules.length == 0) {
        message.setFilterState(false);
        return message;
      }

      boolean allRulesPassed = true;

      for (int i = 0; i < rules.length && allRulesPassed; i++) {
        Rule rule = rules[i];
        allRulesPassed = parseRule(rule, jsonNode);
      }

      message.setFilterState(allRulesPassed);
    } catch (JsonProcessingException e) {
      log.error("Error parsing JSON", e);
      message.setFilterState(false);
    } catch (Exception e) {
      log.error("Error by message processing", e);
      message.setFilterState(false);
    }

    return message;
  }

  private boolean parseRule(Rule rule, JsonNode jsonNode) {
    JsonNode fieldValueNode = jsonNode.get(rule.getFieldName());
    if (fieldValueNode == null || fieldValueNode.isNull()) {
      return false;
    }

    String fieldValue = fieldValueNode.asText();
    String filterValue = rule.getFilterValue();
    String function = rule.getFilterFunctionName().toLowerCase();

    return switch (function) {
      case "equals" -> fieldValue.equals(filterValue);
      case "contains" -> fieldValue.contains(filterValue);
      case "not_equals" -> !fieldValue.equals(filterValue);
      case "not_contains" -> !fieldValue.contains(filterValue);
      default -> throw new UnsupportedOperationException("Unknown filter func: " + function);
      };
    }
  }