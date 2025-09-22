#ifndef PROCESSOR_PIPELINE_H
#define PROCESSOR_PIPELINE_H

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include <rarexsec/EventProcessorStage.h>

namespace proc {

template <typename... Processors>
class ProcessorPipeline : public EventProcessorStage {
  static_assert(sizeof...(Processors) > 0, "ProcessorPipeline requires at least one processor stage");
  static_assert((std::is_base_of_v<EventProcessorStage, Processors> && ...),
                "All processors must derive from EventProcessorStage");

  using ProcessorTuple = std::tuple<std::unique_ptr<Processors>...>;

  public:
    explicit ProcessorPipeline(std::unique_ptr<Processors>... processors)
        : processors_(std::move(processors)...) {}

    ProcessorPipeline(const ProcessorPipeline &) = delete;
    ProcessorPipeline &operator=(const ProcessorPipeline &) = delete;
    ProcessorPipeline(ProcessorPipeline &&) noexcept = default;
    ProcessorPipeline &operator=(ProcessorPipeline &&) noexcept = default;

    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin origin) const override {
        return std::apply(
            [&](auto const &...processor) {
                ROOT::RDF::RNode current = std::move(df);
                ((current = processor->process(current, origin)), ...);
                return current;
            },
            processors_);
    }

  private:
    ProcessorTuple processors_;
};

} // namespace proc

#endif
