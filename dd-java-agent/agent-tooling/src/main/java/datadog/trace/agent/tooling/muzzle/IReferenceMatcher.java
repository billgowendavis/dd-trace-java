package datadog.trace.agent.tooling.muzzle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface IReferenceMatcher {
  boolean matches(ClassLoader loader);

  List<Reference.Mismatch> getMismatchedReferenceSources(ClassLoader loader);

  class ConjunctionReferenceMatcher implements IReferenceMatcher {
    private final IReferenceMatcher m1;
    private final IReferenceMatcher m2;

    public ConjunctionReferenceMatcher(IReferenceMatcher m1, IReferenceMatcher m2) {
      this.m1 = m1;
      this.m2 = m2;
    }

    @Override
    public boolean matches(ClassLoader loader) {
      return m1.matches(loader) && m2.matches(loader);
    }

    @Override
    public List<Reference.Mismatch> getMismatchedReferenceSources(ClassLoader loader) {
      List<Reference.Mismatch> mm1 = m1.getMismatchedReferenceSources(loader);
      List<Reference.Mismatch> mm2 = m2.getMismatchedReferenceSources(loader);
      if (mm2.isEmpty()) {
        return mm1;
      }
      if (mm1.isEmpty()) {
        return mm2;
      }
      List<Reference.Mismatch> allMm = new ArrayList<>();
      allMm.addAll(mm1);
      allMm.addAll(mm2);
      return allMm;
    }
  }

  class DisjunctionReferenceMatcher implements IReferenceMatcher {
    private final IReferenceMatcher m1;
    private final IReferenceMatcher m2;

    public DisjunctionReferenceMatcher(IReferenceMatcher m1, IReferenceMatcher m2) {
      this.m1 = m1;
      this.m2 = m2;
    }

    @Override
    public boolean matches(ClassLoader loader) {
      return this.m1.matches(loader) || this.m2.matches(loader);
    }

    @Override
    public List<Reference.Mismatch> getMismatchedReferenceSources(ClassLoader loader) {
      List<Reference.Mismatch> mm1 = this.m1.getMismatchedReferenceSources(loader);
      List<Reference.Mismatch> mm2 = this.m2.getMismatchedReferenceSources(loader);
      if (!mm1.isEmpty() && !mm2.isEmpty()) {
        return Collections.<Reference.Mismatch>singletonList(new DisjunctionMismatch(mm1, mm2));
      }
      return Collections.emptyList();
    }

    private class DisjunctionMismatch extends Reference.Mismatch {
      final List<Reference.Mismatch> mm1;
      final List<Reference.Mismatch> mm2;

      private DisjunctionMismatch(List<Reference.Mismatch> mm1, List<Reference.Mismatch> mm2) {
        super(new String[0]);
        this.mm1 = mm1;
        this.mm2 = mm2;
      }

      @Override
      public String toString() {
        return getMismatchDetails();
      }

      @Override
      String getMismatchDetails() {
        return "Failed both " + joinDetails(this.mm1) + " and " + joinDetails(this.mm2);
      }

      private String joinDetails(List<Reference.Mismatch> mms) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < mms.size(); i++) {
          if (i != 0) {
            builder.append(", ");
          }
          builder.append(mms.get(i));
        }
        return builder.toString();
      }
    }
  }
}
