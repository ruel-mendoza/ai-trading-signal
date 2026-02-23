import { Button } from "@/components/ui/button";
import { Globe, Bitcoin, Gem, LayoutGrid } from "lucide-react";

const categories = [
  { key: "all", label: "All", icon: LayoutGrid },
  { key: "forex", label: "Forex", icon: Globe },
  { key: "crypto", label: "Crypto", icon: Bitcoin },
  { key: "commodities", label: "Commodities", icon: Gem },
];

interface CategoryFilterProps {
  active: string;
  onChange: (category: string) => void;
}

export function CategoryFilter({ active, onChange }: CategoryFilterProps) {
  return (
    <div className="flex items-center gap-2 flex-wrap" data-testid="filter-categories">
      {categories.map((cat) => {
        const Icon = cat.icon;
        const isActive = active === cat.key;
        return (
          <Button
            key={cat.key}
            variant={isActive ? "default" : "secondary"}
            size="sm"
            onClick={() => onChange(cat.key)}
            className="gap-1.5"
            data-testid={`button-category-${cat.key}`}
          >
            <Icon className="w-3.5 h-3.5" />
            {cat.label}
          </Button>
        );
      })}
    </div>
  );
}
