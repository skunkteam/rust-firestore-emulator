export interface TemplateResult {
  readonly _$litType$: number;
  readonly strings: TemplateStringsArray;
  readonly values: readonly unknown[];
}

export interface RenderOptions {
  host?: object;
  renderBefore?: ChildNode | null;
  creationScope?: { importNode(node: Node, deep?: boolean): Node };
  isConnected?: boolean;
}

export function html(
  strings: TemplateStringsArray,
  ...values: unknown[]
): TemplateResult;

export function svg(
  strings: TemplateStringsArray,
  ...values: unknown[]
): TemplateResult;

export function render(
  value: unknown,
  container: HTMLElement | DocumentFragment,
  options?: RenderOptions
): unknown;

export const nothing: symbol;
export const noChange: symbol;
export const _$LH: Record<string, unknown>;
declare const _default: null;
export default _default;
