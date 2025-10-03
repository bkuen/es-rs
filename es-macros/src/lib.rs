// event_registry_derive/src/lib.rs

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::token::Paren;
use syn::{parse_macro_input, punctuated::Punctuated, Ident, ItemStruct, Path, Result, Token};

struct ViewWrapperArgs {
    views: Punctuated<ViewWithEvent, Token![,]>,
}

impl Parse for ViewWrapperArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let views_ident: Ident = input.parse()?;
        if views_ident != "views" {
            return Err(syn::Error::new_spanned(
                &views_ident,
                "expected `views(...)`",
            ));
        }

        let content;
        syn::parenthesized!(content in input);

        let views = content.parse_terminated(ViewWithEvent::parse, Token![,])?;
        if views.is_empty() {
            return Err(syn::Error::new(content.span(), "views(...) cannot be empty"));
        }

        Ok(ViewWrapperArgs { views })
    }
}

struct ViewWithEvent {
    view: Path,
    event: Path,
}

impl Parse for ViewWithEvent {
    fn parse(input: ParseStream) -> Result<Self> {
        let view: Path = input.parse()?;
        if !input.peek(Paren) {
            return Err(syn::Error::new_spanned(
                &view,
                "expected `(EventType)` after view type, e.g. `PersonView(PersonEvent)`",
            ));
        }
        let content;
        syn::parenthesized!(content in input);
        let event: Path = content.parse()?;
        // Ensure nothing extra inside the parens
        if !content.is_empty() {
            return Err(syn::Error::new(content.span(), "unexpected tokens after event type"));
        }
        Ok(Self { view, event })
    }
}

#[proc_macro_attribute]
pub fn view_wrapper(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    // let attrs = parse_macro_input!(attrs with Punctuated::<Meta, syn::Token![,]>::parse_terminated);
    let ident = &input.ident;
    let vis = &input.vis;
    let ViewWrapperArgs { views } = parse_macro_input!(attrs as ViewWrapperArgs);

    let mut wrapper_variants = Vec::new();
    let mut wrapper_impls = Vec::new();

    for v in &views {
        let view_path = &v.view;
        let event_path = &v.event;
        let variant_ident = v.view.segments.last().unwrap().ident.clone();

        wrapper_variants.push(quote! { #variant_ident(#view_path) });
        wrapper_impls.push(quote! {
            impl From<#view_path> for #ident {
                fn from(view: #view_path) -> Self {
                    Self::#variant_ident(view)
                }
            }

            #[async_trait::async_trait]
            impl HandleEvents<#event_path> for #ident {
                type Error = <#variant_ident as View>::Error;
                type Transaction = <#variant_ident as View>::Transaction;

                async fn handle(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<#event_path>], tx: Option<&Self::Transaction>) -> Result<(), Self::Error> {
                    if let #ident::#variant_ident(view) = self {
                        view.update(aggregate_id, events, tx).await?;
                    }

                    Ok(())
                }
            }
        })
    }

    let expanded = quote! {
        #vis enum #ident {
            #(#wrapper_variants),*
        }

        #(#wrapper_impls)*
    };

    // panic!("{}", expanded);

    expanded.into()
}