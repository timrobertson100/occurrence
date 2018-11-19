package org.gbif.occurrence.processor.interpreting.result;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.gbif.api.model.occurrence.Occurrence;

public class OccurrenceInterpretationResult {

  private final Occurrence original;
  private final Occurrence updated;

  public OccurrenceInterpretationResult(@Nullable Occurrence original, Occurrence updated) {
    this.original = original;
    this.updated = checkNotNull(updated, "updated can't be null");
  }

  public Occurrence getOriginal() {
    return original;
  }

  public Occurrence getUpdated() {
    return updated;
  }
}
