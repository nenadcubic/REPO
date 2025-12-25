import Explorer from "./Explorer";
import SchemaExplorer from "./SchemaExplorer";
import AssocWordNet from "./AssocWordNet";
import NorthwindDataCompare from "./NorthwindDataCompare";

export default function App() {
  const path = window.location.pathname || "/";
  if (path.startsWith("/explorer/schema")) return <SchemaExplorer />;
  if (path.startsWith("/explorer/data")) return <NorthwindDataCompare />;
  if (path.startsWith("/explorer/assoc")) return <AssocWordNet />;
  return <Explorer />;
}
