#include "er/Element.hpp"

namespace er {

static Result<Unit> check_name(std::string_view n) noexcept {
    if (n.size() > 100) return Result<Unit>::err(Errc::kInvalidArg, "Element name exceeds 100 characters");
    return Result<Unit>::ok();
}

Element::Element(std::string name) : name_(std::move(name)) {}

Result<Element> Element::create(std::string name) {
    if (auto ok = check_name(name); !ok) return Result<Element>::err(ok.error().code, ok.error().msg);
    return Result<Element>::ok(Element(std::move(name)));
}

Result<Unit> Element::set_name(std::string name) {
    if (auto ok = check_name(name); !ok) return ok;
    name_ = std::move(name);
    return Result<Unit>::ok();
}

std::string_view Element::name() const noexcept { return name_; }

Flags4096& Element::flags() {
    return flags_;
}

const Flags4096& Element::flags() const {
    return flags_;
}

} // namespace er
